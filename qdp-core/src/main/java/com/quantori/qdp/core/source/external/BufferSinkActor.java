package com.quantori.qdp.core.source.external;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.pattern.StatusReply;
import com.quantori.qdp.core.source.model.molecule.search.SearchResultItem;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferSinkActor extends AbstractBehavior<BufferSinkActor.Command> {

  private static final Logger logger = LoggerFactory.getLogger(BufferSinkActor.class);
  private final BlockingQueue<SearchResultItem> sinkQueue = new LinkedBlockingDeque<>();
  private final int bufferSize;
  private final AtomicBoolean completed = new AtomicBoolean();

  protected BufferSinkActor(ActorContext<Command> context, int bufferSize) {
    super(context);
    completed.set(false);
    this.bufferSize = bufferSize;
    context.getLog().debug("Create search sink actor [bufferSize={}]", bufferSize);
  }

  public static Behavior<Command> create(int bufferSize) {
    return Behaviors.setup(ctx -> new BufferSinkActor(ctx, bufferSize));
  }

  @Override
  public Receive<Command> createReceive() {

    return newReceiveBuilder()
        .onMessage(BufferSinkActor.StreamInitialized.class, this::onInitFlow)
        .onMessage(BufferSinkActor.Item.class, this::onItem)
        .onMessage(BufferSinkActor.StreamCompleted.class, this::onComplete)
        .onMessage(BufferSinkActor.StreamFailure.class, this::onFailure)
        .onMessage(BufferSinkActor.GetItems.class, this::onGetItems)
        .onMessage(BufferSinkActor.Close.class, this::onClose)
        .build();
  }

  private Behavior<Command> onInitFlow(BufferSinkActor.StreamInitialized cmd) {
    logger.debug("Stream initialized");
    cmd.replyTo.tell(BufferSinkActor.Ack.INSTANCE);

    return this;
  }

  private Behavior<Command> onItem(BufferSinkActor.Item element) {
    storeItem(element, element.flowReference);
    element.replyTo.tell(BufferSinkActor.Ack.INSTANCE);

    return this;
  }

  private Behavior<Command> onComplete(BufferSinkActor.StreamCompleted cmd) {
    logger.debug("Stream completed");
    completed.set(true);
    cmd.flowReference.tell(new DataSourceActor.CompletedFlow());
    return this;
  }

  private Behavior<Command> onFailure(BufferSinkActor.StreamFailure failed) {
    logger.error("Stream failed!", failed.cause);

    return this;
  }

  private Behavior<Command> onGetItems(BufferSinkActor.GetItems getItems) {
    CompletableFuture.runAsync(() -> {
      List<SearchResultItem> items = new ArrayList<>(getItems.amount);
      try {
        SearchResultItem item;
        while (items.size() < getItems.amount
            && (!sinkQueue.isEmpty() || !completed.get())
            && (item = sinkQueue.poll(2000, TimeUnit.MILLISECONDS)) != null) {
          items.add(item);
        }
      } catch (InterruptedException e) {
        logger.error("The search result waiting thread was interrupted", e);
        Thread.currentThread().interrupt();
      }

      logger.debug("GetItems asks for {} items, returned {} items, in buffer {}",
          getItems.amount, items.size(), sinkQueue.size());
      BufferSinkActor.GetItemsResponse response = new BufferSinkActor.GetItemsResponse(items, sinkQueue.size());
      getItems.replyTo.tell(StatusReply.success(response));
    }, getContext().getExecutionContext());

    return this;
  }

  private Behavior<Command> onClose(BufferSinkActor.Close cmd) {
    logger.debug("Close actor ask");
    sinkQueue.clear();
    return Behaviors.stopped();
  }

  private void storeItem(Item element, ActorRef<DataSourceActor.Command> flowReference) {
    logger.trace("Received element: {}", element);
    try {
      sinkQueue.add(element.item);
      if (sinkQueue.size() >= bufferSize) {
        pauseFlow(flowReference);
      }
      logger.trace("Buffer queue after add size: {}", sinkQueue.size());
    } catch (RuntimeException e) {
      logger.error("Molecule search consumer failed to add data: {}", element, e);
      throw e;
    }
  }

  private void pauseFlow(ActorRef<DataSourceActor.Command> flowReference) {
    logger.debug("The flow need to be paused, size: {}", sinkQueue.size());

    DataSourceActor.PauseFlow message = new DataSourceActor.PauseFlow();
    flowReference.tell(message);
  }

  public enum Ack {
    INSTANCE
  }

  public interface Command {
  }

  public static class GetItemsResponse {
    private final List<SearchResultItem> items;
    private final int bufferSize;

    public GetItemsResponse(List<SearchResultItem> items, int bufferSize) {
      this.items = items;
      this.bufferSize = bufferSize;
    }

    public List<SearchResultItem> getItems() {
      return items;
    }

    public int getBufferSize() {
      return bufferSize;
    }
  }

  static class StreamInitialized implements Command {
    private final ActorRef<Ack> replyTo;

    StreamInitialized(ActorRef<Ack> replyTo) {
      this.replyTo = replyTo;
    }
  }

  static class StreamCompleted implements Command {
    private final ActorRef<DataSourceActor.Command> flowReference;

    StreamCompleted(ActorRef<DataSourceActor.Command> flowReference) {
      this.flowReference = flowReference;
    }
  }

  static class Close implements Command {
  }

  static class Item implements Command {
    private final ActorRef<Ack> replyTo;
    private final SearchResultItem item;
    private final ActorRef<DataSourceActor.Command> flowReference;

    Item(ActorRef<Ack> replyTo, SearchResultItem item, ActorRef<DataSourceActor.Command> flowReference) {
      this.replyTo = replyTo;
      this.item = item;
      this.flowReference = flowReference;
    }
  }

  static class GetItems implements Command {
    private final ActorRef<StatusReply<GetItemsResponse>> replyTo;
    private final int amount;

    GetItems(ActorRef<StatusReply<GetItemsResponse>> replyTo, int amount) {
      this.replyTo = replyTo;
      this.amount = amount;
    }
  }

  static class StreamFailure implements Command {
    private final Throwable cause;

    StreamFailure(Throwable cause) {
      this.cause = cause;
    }
  }
}
