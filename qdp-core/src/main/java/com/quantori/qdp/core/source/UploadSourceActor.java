package com.quantori.qdp.core.source;

import static java.util.concurrent.CompletableFuture.completedStage;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.pattern.StatusReply;
import com.quantori.qdp.api.model.core.DataSource;
import com.quantori.qdp.api.model.core.DataStorage;
import com.quantori.qdp.api.model.core.DataUploadItem;
import com.quantori.qdp.api.model.core.PipelineStatistics;
import com.quantori.qdp.api.model.core.StorageUploadItem;
import com.quantori.qdp.api.model.core.TransformationStep;
import com.quantori.qdp.api.service.ItemWriter;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UploadSourceActor<D extends DataUploadItem, U extends StorageUploadItem>
    extends AbstractBehavior<UploadSourceActor.Command> {
  protected final DataStorage<U, ?, ?> storage;
  private final Queue<LoadFromDataSource<D, U>> uploadCmdQueue = new LinkedList<>();
  private final int maxUploadCount;
  private final AtomicInteger uploadCount = new AtomicInteger();

  private UploadSourceActor(ActorContext<Command> context, DataStorage<U, ?, ?> storage, int maxUploadCount) {
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
        .onMessage(UploadSourceActor.UploadComplete.class, this::onUploadComplete)
        .onMessage(UploadSourceActor.LoadFromDataSource.class, this::onLoadFromDataSource)
        .build();
  }

  public static <U extends StorageUploadItem> Behavior<Command> create(DataStorage<U, ?, ?> storage, int maxUploads) {
    return Behaviors.setup(ctx -> new UploadSourceActor<>(ctx, storage, maxUploads));
  }

  private Behavior<UploadSourceActor.Command> onUploadComplete(UploadComplete msg) {
    log.info("Received upload complete: {}", msg);

    uploadCount.decrementAndGet();

    if (!uploadCmdQueue.isEmpty() && uploadCount.get() < maxUploadCount) {
      LoadFromDataSource<D, U> cmd = uploadCmdQueue.poll();

      startUpload(cmd);
    }
    return this;
  }

  private void startUpload(LoadFromDataSource<D, U> cmd) {
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

  private Behavior<UploadSourceActor.Command> onLoadFromDataSource(LoadFromDataSource<D, U> cmd) {
    log.info("Received load from data source command: {}", cmd);

    if (uploadCount.get() < maxUploadCount) {
      startUpload(cmd);
    } else {
      uploadCmdQueue.add(cmd);
      log.info("SearchItem upload added to the queue: {}", cmd);
    }

    return this;
  }

  private CompletionStage<PipelineStatistics> loadFromDataSource(LoadFromDataSource<D, U> command) {
    return completedStage(command).thenComposeAsync(cmd -> {
      final ItemWriter<U> itemWriter = storage.itemWriter(cmd.libraryId);
      final DataSource<D> dataSource = cmd.dataSource;

      final var loader = new Loader<D, U>(getContext().getSystem());
      return loader.loadStorageItems(dataSource, cmd.transformation, itemWriter::write)
          .whenComplete((done, error) -> close(dataSource))
          .whenComplete((done, error) -> close(itemWriter));
    });
  }

  private void close(final AutoCloseable closeable) {
    try {
      closeable.close();
    } catch (Exception e) {
      log.error("Failed to close resource: {}", closeable, e);
    }
  }

  public interface Command {
  }

  @AllArgsConstructor
  public static class LoadFromDataSource<D extends DataUploadItem, U extends StorageUploadItem> implements Command {
    public final String libraryId;
    public final DataSource<D> dataSource;
    public final TransformationStep<D, U> transformation;
    public final ActorRef<StatusReply<PipelineStatistics>> replyTo;

    @Override
    public String toString() {
      return getClass().getSimpleName() + " [" + "dataSource=" + dataSource + ", libraryId='" + libraryId + '\''
          + ']';
    }
  }

  @AllArgsConstructor
  public static class CreateSearch implements Command {
    public final ActorRef<StatusReply<ActorRef<SearchActor.Command>>> replyTo;
  }

  @AllArgsConstructor
  public static class UploadComplete implements Command {
    public final LoadFromDataSource<?, ?> cause;

    @Override
    public String toString() {
      return getClass().getSimpleName() + " [" + "cause=" + cause + ']';
    }
  }

}
