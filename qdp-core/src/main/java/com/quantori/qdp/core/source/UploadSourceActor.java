package com.quantori.qdp.core.source;

import static java.util.concurrent.CompletableFuture.completedStage;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import akka.pattern.StatusReply;
import com.quantori.qdp.core.source.model.DataLibrary;
import com.quantori.qdp.core.source.model.DataLibraryType;
import com.quantori.qdp.core.source.model.DataLoader;
import com.quantori.qdp.core.source.model.DataSource;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.PipelineStatistics;
import com.quantori.qdp.core.source.model.StorageItem;
import com.quantori.qdp.core.source.model.TransformationStep;
import com.quantori.qdp.core.source.model.UploadItem;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadSourceActor extends AbstractBehavior<UploadSourceActor.Command> {
  protected final DataStorage storage;
  private final Queue<LoadFromDataSource> uploadCmdQueue = new LinkedList<>();
  private final int maxUploadCount;
  private final AtomicInteger uploadCount = new AtomicInteger();

  protected UploadSourceActor(ActorContext<Command> context, DataStorage storage, int maxUploadCount) {
    super(context);
    if (maxUploadCount <= 0) {
      throw new IllegalArgumentException(
          "Expected max uploads parameter greater then 0 but received " + maxUploadCount);
    }
    this.maxUploadCount = maxUploadCount;
    this.storage = storage;
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(UploadSourceActor.LoadFromDataSource.class, this::onLoadFromDataSource)
        .onMessage(GetLibraries.class, this::onGetLibrary)
        .onMessage(CreateLibrary.class, this::onCreateLibrary)
        .onMessage(UploadSourceActor.FindLibrary.class, this::onFindLibrary)
        .onMessage(UploadSourceActor.UploadComplete.class, this::onUploadComplete)
        .build();
  }

  private Behavior<UploadSourceActor.Command> onGetLibrary(final GetLibraries cmd) {
    getLibraries().whenComplete((list, t) -> {
      if (list != null) {
        cmd.replyTo.tell(StatusReply.success(list));
      } else {
        cmd.replyTo.tell(StatusReply.error(t));
      }
    });
    return this;
  }

  public static Behavior<Command> create(DataStorage storage, int maxUploads) {
    return Behaviors.setup(ctx -> new UploadSourceActor(ctx, storage, maxUploads));
  }

  private CompletionStage<List<DataLibrary>> getLibraries() {
    return CompletableFuture.supplyAsync(storage::getLibraries);
  }

  private Behavior<UploadSourceActor.Command> onCreateLibrary(CreateLibrary cmd) {
    createLibrary(cmd).whenComplete((library, throwable) -> {
      if (throwable == null) {
        cmd.replyTo.tell(StatusReply.success(library));
      } else {
        cmd.replyTo.tell(StatusReply.error(throwable));
      }
    });
    return this;
  }

  private Behavior<UploadSourceActor.Command> onFindLibrary(FindLibrary cmd) {
    log.debug("Received find library command [cmd={}]", cmd);
    findLibrary(cmd).whenComplete((library, throwable) -> {
      if (throwable == null) {
        cmd.replyTo.tell(StatusReply.success(library));
      } else {
        cmd.replyTo.tell(StatusReply.error(throwable));
      }
    });
    return this;
  }

  private CompletionStage<DataLibrary> createLibrary(CreateLibrary cmd) {
    return CompletableFuture.supplyAsync(() -> storage.createLibrary(cmd.dataLibrary));
  }

  private CompletionStage<DataLibrary> findLibrary(FindLibrary cmd) {
    return CompletableFuture.supplyAsync(() -> storage.findLibrary(cmd.libraryName, cmd.libraryType));
  }

  private Behavior<UploadSourceActor.Command> onUploadComplete(UploadComplete msg) {
    log.info("Received upload complete: {}", msg);

    uploadCount.decrementAndGet();

    if (!uploadCmdQueue.isEmpty() && uploadCount.get() < maxUploadCount) {
      LoadFromDataSource cmd = uploadCmdQueue.poll();

      startUpload(cmd);
    }
    return this;
  }

  private void startUpload(LoadFromDataSource cmd) {
    uploadCount.incrementAndGet();
    final ActorRef<Command> self = getContext().getSelf();

    log.info("Starting molecule upload: {}", cmd);

    loadFromDataSource(cmd).toCompletableFuture().whenComplete((stat, throwable) -> {
      if (throwable != null) {
        log.error("Failed to load molecules from data source: {}", cmd, throwable);
        cmd.replyTo.tell(StatusReply.error(throwable));
      } else {
        log.info("Data has been loaded to the storage: {}", cmd);
        cmd.replyTo.tell(StatusReply.success(stat));
      }
      self.tell(new UploadComplete(cmd));
    });
  }

  private Behavior<UploadSourceActor.Command> onLoadFromDataSource(LoadFromDataSource cmd) {
    log.info("Received load from data source command: {}", cmd);

    if (uploadCount.get() < maxUploadCount) {
      startUpload(cmd);
    } else {
      uploadCmdQueue.add(cmd);
      log.info("SearchItem upload added to the queue: {}", cmd);
    }

    return this;
  }

  private CompletionStage<PipelineStatistics> loadFromDataSource(LoadFromDataSource command) {
    return completedStage(command).thenComposeAsync(cmd -> {
      final DataLoader storageLoader = storage.dataLoader(cmd.libraryId);
      final DataSource<UploadItem> dataSource = cmd.dataSource;

      final var loader = new Loader(getContext().getSystem());
      return loader.loadStorageItems(dataSource, cmd.transformation, storageLoader::add)
          .whenComplete((done, error) -> close(dataSource))
          .whenComplete((done, error) -> close(storageLoader));
    });
  }

  private void registerSearchActor(ActorRef<StatusReply<ActorRef<SearchActor.Command>>> replyTo,
                                   ActorRef<SearchActor.Command> searchRef, String searchId) {
    ServiceKey<SearchActor.Command> serviceKey = SearchActor.searchActorKey(searchId);

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
        .tell(Receptionist.register(SearchActor.searchActorsKey, searchRef));
  }

  private void close(final AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (Exception e) {
      log.error("Failed to close resource: {}", closeable, e);
    }
  }

  public abstract static class Command {
  }

  @AllArgsConstructor
  public static class LoadFromDataSource extends Command {
    public final String libraryId;
    public final DataSource<UploadItem> dataSource;
    public final TransformationStep<UploadItem, StorageItem> transformation;
    public final ActorRef<StatusReply<PipelineStatistics>> replyTo;

    @Override
    public String toString() {
      return getClass().getSimpleName() + " [" + "dataSource=" + dataSource + ", libraryId='" + libraryId + '\''
          + ']';
    }
  }

  @AllArgsConstructor
  public static class CreateSearch extends Command {
    public final ActorRef<StatusReply<ActorRef<SearchActor.Command>>> replyTo;
  }

  @AllArgsConstructor
  public static class GetLibraries extends Command {
    public final ActorRef<StatusReply<List<DataLibrary>>> replyTo;
  }

  @AllArgsConstructor
  public static class CreateLibrary extends Command {
    public final ActorRef<StatusReply<DataLibrary>> replyTo;
    public final DataLibrary dataLibrary;
  }

  @AllArgsConstructor
  public static class UploadComplete extends Command {
    public final LoadFromDataSource cause;

    @Override
    public String toString() {
      return getClass().getSimpleName() + " [" + "cause=" + cause + ']';
    }
  }

  @AllArgsConstructor
  public static class FindLibrary extends Command {
    public final ActorRef<StatusReply<DataLibrary>> replyTo;
    public final String libraryName;
    public final DataLibraryType libraryType;
  }
}
