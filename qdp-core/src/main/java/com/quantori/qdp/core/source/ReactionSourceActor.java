package com.quantori.qdp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;
import akka.pattern.StatusReply;
import com.quantori.qdp.core.source.external.ExternalReactionSourceActor;
import com.quantori.qdp.core.source.model.DataLibrary;
import com.quantori.qdp.core.source.model.DataLibraryType;
import com.quantori.qdp.core.source.model.DataSource;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.PipelineStatistics;
import com.quantori.qdp.core.source.model.StorageType;
import com.quantori.qdp.core.source.model.TransformationStep;
import com.quantori.qdp.core.source.model.reaction.Reaction;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;

public abstract class ReactionSourceActor extends AbstractBehavior<ReactionSourceActor.Command> {
  protected final String storageName;

  private final Queue<ReactionSourceActor.LoadFromDataSource<?>> uploadCmdQueue = new LinkedList<>();
  private final int maxUploadCount;
  private final AtomicInteger uploadCount = new AtomicInteger();

  protected ReactionSourceActor(ActorContext<ReactionSourceActor.Command> context, String storageName,
                                int maxUploadCount) {
    super(context);
    this.storageName = storageName;
    if (maxUploadCount <= 0) {
      throw new IllegalArgumentException(
          "Expected max uploads parameter greater then 0 but received " + maxUploadCount);
    }
    this.maxUploadCount = maxUploadCount;
  }


  @Override
  public Receive<ReactionSourceActor.Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(ReactionSourceActor.LoadFromDataSource.class, this::onLoadFromDataSource)
        .onMessage(GetLibraries.class, this::onGetLibraries)
        .onMessage(FindLibrary.class, this::onFindLibrary)
        .onMessage(UploadComplete.class, this::onUploadComplete)
        .build();
  }
  protected abstract CompletionStage<DataLibrary> findLibrary(ReactionSourceActor.FindLibrary cmd);
  protected abstract CompletionStage<List<DataLibrary>> getLibraries();
  protected abstract <S> CompletionStage<PipelineStatistics> loadFromDataSource(
      ReactionSourceActor.LoadFromDataSource<S> loadFileCmd);

  private Behavior<ReactionSourceActor.Command> onGetLibraries(final ReactionSourceActor.GetLibraries cmd) {
    getLibraries().whenComplete((list, t) -> {
      if (list != null) {
        cmd.replyTo.tell(StatusReply.success(list));
      } else {
        cmd.replyTo.tell(StatusReply.error(t));
      }
    });
    return this;
  }

  private Behavior<ReactionSourceActor.Command> onFindLibrary(ReactionSourceActor.FindLibrary cmd) {
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

  public static Behavior<ReactionSourceActor.Command> create(StorageType storageType, String storageName,
                                                             int maxUploads, DataStorage<Reaction> storage) {
    if (storageType == StorageType.EXTERNAL) {
      return ExternalReactionSourceActor.create(storageName, maxUploads, storage);
    }
    throw new RuntimeException("Unexpected source type: " + storageType);
  }

  public static Behavior<ReactionSourceActor.Command> create(StorageType storageType, String storageName,
                                                             int maxUploads) {
    if (storageType == StorageType.MEMORY) { //TODO inMemoryReactionSourceActor
     // return InMemorySourceActor.create(storageName, maxUploads);
    }
    throw new RuntimeException("Unexpected source type: " + storageType);
  }
  protected void close(final AutoCloseable closeable, final Logger logger) {
    try {
      closeable.close();
    } catch (Exception e) {
      logger.error("Failed to close resource: {}", closeable, e);
    }
  }

  private Behavior<ReactionSourceActor.Command> onUploadComplete(ReactionSourceActor.UploadComplete msg) {
    getContext().getLog().info("Received upload complete: {}", msg);

    uploadCount.decrementAndGet();

    if (!uploadCmdQueue.isEmpty() && uploadCount.get() < maxUploadCount) {
      ReactionSourceActor.LoadFromDataSource<?> cmd = uploadCmdQueue.poll();

      startUpload(cmd);
    }
    return this;
  }

  private Behavior<ReactionSourceActor.Command> onLoadFromDataSource(ReactionSourceActor.LoadFromDataSource<?> cmd) {
    getContext().getLog().info("Received load from data source command: {}", cmd);

    if (uploadCount.get() < maxUploadCount) {
      startUpload(cmd);
    } else {
      uploadCmdQueue.add(cmd);
      getContext().getLog().info("Reaction upload added to the queue: {}", cmd);
    }

    return this;
  }

  private void startUpload(ReactionSourceActor.LoadFromDataSource<?> cmd) {
    uploadCount.incrementAndGet();
    // Passing logger as value due to Actor context doesn't available outside of actor (other thread).
    final var logger = getContext().getLog();
    final ActorRef<ReactionSourceActor.Command> self = getContext().getSelf();

    logger.info("Starting reaction upload: {}", cmd);

    loadFromDataSource(cmd).toCompletableFuture().whenComplete((stat, t) -> {
      if (t != null) {
        logger.error("Failed to load reaction from data source: {}", cmd, t);
        cmd.replyTo.tell(StatusReply.error(t));
      } else {
        logger.info("Data has been loaded to the storage: {}", cmd);
        cmd.replyTo.tell(StatusReply.success(stat));
      }
      self.tell(new ReactionSourceActor.UploadComplete(cmd));
    });
  }

  public abstract static class Command {
  }

  public static class GetLibraries extends Command {
    public final ActorRef<StatusReply<List<DataLibrary>>> replyTo;

    public GetLibraries(ActorRef<StatusReply<List<DataLibrary>>> replyTo) {
      this.replyTo = replyTo;
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

  public static class LoadFromDataSource<S> extends Command {
    public final DataSource<S> dataSource;
    public final TransformationStep<S, Reaction> transformation;
    public final ActorRef<StatusReply<PipelineStatistics>> replyTo;
    public final String libraryId;

    public LoadFromDataSource(String libraryId,
                              DataSource<S> dataSource,
                              TransformationStep<S, Reaction> transformation,
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

  public static class UploadComplete extends Command {
    public final ReactionSourceActor.LoadFromDataSource<?> cause;

    public UploadComplete(ReactionSourceActor.LoadFromDataSource<?> cause) {
      this.cause = cause;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + " [" + "cause=" + cause + ']';
    }
  }

}
