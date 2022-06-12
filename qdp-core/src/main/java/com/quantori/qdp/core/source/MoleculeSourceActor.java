package com.quantori.qdp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import akka.pattern.StatusReply;
import com.quantori.qdp.core.source.external.ExternalSourceActor;
import com.quantori.qdp.core.source.memory.InMemorySourceActor;
import com.quantori.qdp.core.source.model.DataLibrary;
import com.quantori.qdp.core.source.model.DataLibraryType;
import com.quantori.qdp.core.source.model.DataSource;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.PipelineStatistics;
import com.quantori.qdp.core.source.model.StorageType;
import com.quantori.qdp.core.source.model.TransformationStep;
import com.quantori.qdp.core.source.model.molecule.Molecule;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;

public abstract class MoleculeSourceActor extends AbstractBehavior<MoleculeSourceActor.Command> {
  protected final String storageName;

  private final Queue<LoadFromDataSource<?>> uploadCmdQueue = new LinkedList<>();
  private final int maxUploadCount;
  private final AtomicInteger uploadCount = new AtomicInteger();

  protected MoleculeSourceActor(ActorContext<Command> context, String storageName, int maxUploadCount) {
    super(context);
    this.storageName = storageName;
    if (maxUploadCount <= 0) {
      throw new IllegalArgumentException(
          "Expected max uploads parameter greater then 0 but received " + maxUploadCount);
    }
    this.maxUploadCount = maxUploadCount;
  }

  public static Behavior<MoleculeSourceActor.Command> create(StorageType storageType, String storageName,
                                                             int maxUploads) {
    if (storageType == StorageType.MEMORY) {
      return InMemorySourceActor.create(storageName, maxUploads);
    }
    throw new RuntimeException("Unexpected source type: " + storageType);
  }

  public static Behavior<MoleculeSourceActor.Command> create(StorageType storageType, String storageName,
                                                             int maxUploads, DataStorage<Molecule> storage) {
    if (storageType == StorageType.EXTERNAL) {
      return ExternalSourceActor.create(storageName, maxUploads, storage);
    }
    throw new RuntimeException("Unexpected source type: " + storageType);
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(MoleculeSourceActor.LoadFromDataSource.class, this::onLoadFromDataSource)
        .onMessage(MoleculeSourceActor.CreateSearch.class, this::onCreateSearch)
        .onMessage(GetLibraries.class, this::onGetLibrary)
        .onMessage(CreateLibrary.class, this::onCreateLibrary)
        .onMessage(MoleculeSourceActor.FindLibrary.class, this::onFindLibrary)
        .onMessage(MoleculeSourceActor.UploadComplete.class, this::onUploadComplete)
        .build();
  }

  private Behavior<MoleculeSourceActor.Command> onGetLibrary(final GetLibraries cmd) {
    getLibraries().whenComplete((list, t) -> {
      if (list != null) {
        cmd.replyTo.tell(StatusReply.success(list));
      } else {
        cmd.replyTo.tell(StatusReply.error(t));
      }
    });
    return this;
  }

  protected abstract CompletionStage<List<DataLibrary>> getLibraries();

  private Behavior<MoleculeSourceActor.Command> onCreateLibrary(CreateLibrary cmd) {
    var log = getContext().getLog();
    log.debug("Received create library command [storageName={}, cmd={}]", storageName, cmd);
    createLibrary(cmd).whenComplete((library, t) -> {
      if (t == null) {
        log.debug("Library successfully created [storageName={}, libraryName={}]",
            storageName, cmd.dataLibrary.getName());
        cmd.replyTo.tell(StatusReply.success(library));
      } else {
        log.error("Failed to create storage library [storageName=" + storageName + ", libraryName="
            + cmd.dataLibrary.getName() + "]", t);
        cmd.replyTo.tell(StatusReply.error(t));
      }
    });
    return this;
  }

  private Behavior<MoleculeSourceActor.Command> onFindLibrary(FindLibrary cmd) {
    var log = getContext().getLog();
    log.debug("Received find library command [cmd={}]", cmd);
    findLibrary(cmd).whenComplete((library, t) -> {
      if (t == null) {
        log.debug("Library found [storageName={}, libraryName={}, libraryType={}]", storageName, cmd.libraryName,
            cmd.libraryType);
        cmd.replyTo.tell(StatusReply.success(library));
      } else {
        log.error("Failed to find or create library [storageName={}, libraryName={}, libraryType={}]", storageName,
            cmd.libraryName, cmd.libraryType, t);
        cmd.replyTo.tell(StatusReply.error(t));
      }
    });
    return this;
  }

  protected abstract CompletionStage<DataLibrary> createLibrary(CreateLibrary cmd);

  protected abstract CompletionStage<DataLibrary> findLibrary(FindLibrary cmd);

  private Behavior<MoleculeSourceActor.Command> onUploadComplete(UploadComplete msg) {
    getContext().getLog().info("Received upload complete: {}", msg);

    uploadCount.decrementAndGet();

    if (!uploadCmdQueue.isEmpty() && uploadCount.get() < maxUploadCount) {
      LoadFromDataSource<?> cmd = uploadCmdQueue.poll();

      startUpload(cmd);
    }
    return this;
  }

  private void startUpload(LoadFromDataSource<?> cmd) {
    uploadCount.incrementAndGet();
    // Passing logger as value due to Actor context doesn't available outside of actor (other thread).
    final var logger = getContext().getLog();
    final ActorRef<Command> self = getContext().getSelf();

    logger.info("Starting molecule upload: {}", cmd);

    loadFromDataSource(cmd).toCompletableFuture().whenComplete((stat, t) -> {
      if (t != null) {
        logger.error("Failed to load molecules from data source: {}", cmd, t);
        cmd.replyTo.tell(StatusReply.error(t));
      } else {
        logger.info("Data has been loaded to the storage: {}", cmd);
        cmd.replyTo.tell(StatusReply.success(stat));
      }
      self.tell(new UploadComplete(cmd));
    });
  }

  private Behavior<MoleculeSourceActor.Command> onLoadFromDataSource(LoadFromDataSource<?> cmd) {
    getContext().getLog().info("Received load from data source command: {}", cmd);

    if (uploadCount.get() < maxUploadCount) {
      startUpload(cmd);
    } else {
      uploadCmdQueue.add(cmd);
      getContext().getLog().info("Molecule upload added to the queue: {}", cmd);
    }

    return this;
  }

  protected abstract <S> CompletionStage<PipelineStatistics> loadFromDataSource(LoadFromDataSource<S> loadFileCmd);

  private Behavior<MoleculeSourceActor.Command> onCreateSearch(CreateSearch createSearchCmd) {
    String searchId = UUID.randomUUID().toString();
    ActorRef<MoleculeSearchActor.Command> searchRef = createSearchActor(createSearchCmd, searchId);
    registerSearchActor(createSearchCmd.replyTo, searchRef, searchId);

    return this;
  }

  private void registerSearchActor(ActorRef<StatusReply<ActorRef<MoleculeSearchActor.Command>>> replyTo,
                                   ActorRef<MoleculeSearchActor.Command> searchRef,
                                   String searchId) {
    ServiceKey<MoleculeSearchActor.Command> serviceKey = MoleculeSearchActor.searchActorKey(searchId);

    Behavior<Receptionist.Registered> listener = Behaviors.receive(Receptionist.Registered.class)
        .onMessage(Receptionist.Registered.class, message -> {
          if (message.getKey().id().equals(searchId)) {
            replyTo.tell(StatusReply.success(searchRef));
            return Behaviors.stopped();
          }

          return Behaviors.same();
        }).build();

    ActorRef<Receptionist.Registered> refListener = getContext().spawn(listener, "registerer-" + UUID.randomUUID());

    getContext().getSystem().receptionist()
        .tell(Receptionist.register(serviceKey, searchRef, refListener));
    getContext().getSystem().receptionist()
        .tell(Receptionist.register(MoleculeSearchActor.searchActorsKey, searchRef));
  }

  protected abstract ActorRef<MoleculeSearchActor.Command> createSearchActor(CreateSearch createSearchCmd, String searchId);
  protected void close(final AutoCloseable closeable, final Logger logger) {
    try {
      closeable.close();
    } catch (Exception e) {
      logger.error("Failed to close resource: {}", closeable, e);
    }
  }

  public abstract static class Command {
  }

  public static class LoadFromDataSource<S> extends Command {
    public final DataSource<S> dataSource;
    public final TransformationStep<S, Molecule> transformation;
    public final ActorRef<StatusReply<PipelineStatistics>> replyTo;
    public final String libraryId;

    public LoadFromDataSource(String libraryId,
                              DataSource<S> dataSource,
                              TransformationStep<S, Molecule> transformation,
                              ActorRef<StatusReply<PipelineStatistics>> replyTo) {
      this.dataSource = dataSource;
      this.transformation = transformation;
      this.libraryId = libraryId;
      this.replyTo = replyTo;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + " [" + "dataSource=" + dataSource + ", libraryId='" + libraryId + '\''
          + ']';
    }
  }

  public static class CreateSearch extends Command {
    public final ActorRef<StatusReply<ActorRef<MoleculeSearchActor.Command>>> replyTo;

    public CreateSearch(ActorRef<StatusReply<ActorRef<MoleculeSearchActor.Command>>> replyTo) {
      this.replyTo = replyTo;
    }
  }

  public static class GetLibraries extends Command {
    public final ActorRef<StatusReply<List<DataLibrary>>> replyTo;

    public GetLibraries(ActorRef<StatusReply<List<DataLibrary>>> replyTo) {
      this.replyTo = replyTo;
    }
  }

  public static class CreateLibrary extends Command {
    public final ActorRef<StatusReply<DataLibrary>> replyTo;
    public final DataLibrary dataLibrary;

    public CreateLibrary(ActorRef<StatusReply<DataLibrary>> replyTo, DataLibrary dataLibrary) {
      this.replyTo = replyTo;
      this.dataLibrary = dataLibrary;
    }
  }

  public static class UploadComplete extends Command {
    public final LoadFromDataSource<?> cause;

    public UploadComplete(LoadFromDataSource<?> cause) {
      this.cause = cause;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + " [" + "cause=" + cause + ']';
    }
  }

  public static class FindLibrary extends Command {
    public final ActorRef<StatusReply<DataLibrary>> replyTo;
    public final String libraryName;
    public final DataLibraryType libraryType;

    public FindLibrary(final ActorRef<StatusReply<DataLibrary>> replyTo, final String libraryName,
                       final DataLibraryType libraryType) {
      this.replyTo = replyTo;
      this.libraryName = libraryName;
      this.libraryType = libraryType;
    }
  }
}
