package com.quantori.qdp.core.source;

import akka.NotUsed;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.ReceiveBuilder;
import akka.pattern.StatusReply;
import akka.stream.javadsl.Sink;
import akka.stream.typed.javadsl.ActorSink;
import com.quantori.qdp.api.model.core.FetchWaitMode;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class BufferSinkActor<S> extends AbstractBehavior<BufferSinkActor.Command> {
  private final Deque<S> buffer;
  private final int bufferLimit;
  private final int fetchLimit;
  private final AtomicInteger fetchCounter = new AtomicInteger(0);
  private boolean fetchFinished = false;
  private Throwable error;

  private ActorRef<StatusReply<BufferSinkActor.GetItemsResponse<S>>> runningSearchReplyTo;
  private int runningSearchLimit;
  private ActorRef<BufferSinkActor.Ack> ackActor;

  BufferSinkActor(ActorContext<Command> context, int bufferLimit, int fetchLimit) {
    super(context);
    this.bufferLimit = bufferLimit;
    this.fetchLimit = fetchLimit;
    buffer = new ArrayDeque<>(bufferLimit);
    log.debug("Create search sink actor [bufferSize={}]", bufferLimit);
  }

  static Behavior<Command> create(int bufferSize, int fetchLimit) {
    return Behaviors.setup(ctx -> new BufferSinkActor<>(ctx, bufferSize, fetchLimit));
  }

  static <S> Sink<S, NotUsed> getSink(ActorRef<DataSourceActor.Command> actorRef,
                                      ActorRef<BufferSinkActor.Command> bufferActorSinkRef) {
    return ActorSink.actorRefWithBackpressure(
        bufferActorSinkRef,
        (replyTo, item) -> new BufferSinkActor.Item<>(replyTo, item, actorRef),
        BufferSinkActor.StreamInitialized::new,
        BufferSinkActor.Ack.INSTANCE,
        new BufferSinkActor.StreamCompleted(actorRef),
        BufferSinkActor.StreamFailure::new
    );
  }

  @Override
  public Receive<Command> createReceive() {
    ReceiveBuilder<Command> builder = newReceiveBuilder();
    builder.onMessage(BufferSinkActor.StreamInitialized.class, this::onInitFlow);
    builder.onMessage(BufferSinkActor.Item.class, this::onItem);
    builder.onMessage(BufferSinkActor.GetItems.class, this::onGetItems);
    builder.onMessage(BufferSinkActor.StreamCompleted.class, this::onComplete);
    builder.onMessage(BufferSinkActor.StreamFailure.class, this::onFailure);
    builder.onMessage(BufferSinkActor.Close.class, this::onClose);
    return builder.build();
  }

  private Behavior<Command> onInitFlow(BufferSinkActor.StreamInitialized cmd) {
    log.debug("Stream initialized");
    cmd.replyTo.tell(BufferSinkActor.Ack.INSTANCE);
    return this;
  }

  private Behavior<Command> onItem(BufferSinkActor.Item<S> element) {
    buffer.add(element.item);

    if (runningSearchReplyTo != null && isBufferContainsEnoughItems(runningSearchLimit)) {
      List<S> items = take(runningSearchLimit);
      BufferSinkActor.GetItemsResponse<S> response = new BufferSinkActor
          .GetItemsResponse<>(new ArrayList<>(items), searchCompleted(), fetchFinished);
      runningSearchReplyTo.tell(StatusReply.success(response));
      runningSearchLimit = 0;
      runningSearchReplyTo = null;
    }

    if (fetchLimit > 0) {
      if (fetchCounter.incrementAndGet() == fetchLimit) {
        log.debug("Stream fetch limit reached");
        ackActor = element.replyTo;
        // stop draining elements when buffer is full
        element.flowReference.tell(new DataSourceActor.CompletedFlow());
      }
      element.replyTo.tell(BufferSinkActor.Ack.INSTANCE);
    } else {
      if (buffer.size() < this.bufferLimit) {
        // keep reading elements till we fill the buffer
        element.replyTo.tell(BufferSinkActor.Ack.INSTANCE);
      } else {
        // suspend fetching more elements
        ackActor = element.replyTo;
      }
    }
    return this;
  }

  private Behavior<Command> onComplete(BufferSinkActor.StreamCompleted cmd) {
    log.debug("Stream completed");
    fetchFinished = true;
    if (runningSearchReplyTo != null) {
      List<S> response = take(Math.min(buffer.size(), runningSearchLimit));
      BufferSinkActor.GetItemsResponse<S> result = new BufferSinkActor
          .GetItemsResponse<>(response, searchCompleted(), fetchFinished);
      runningSearchReplyTo.tell(StatusReply.success(result));
      runningSearchLimit = 0;
      runningSearchReplyTo = null;
    }
    if (buffer.isEmpty()) {
      cmd.flowReference.tell(new DataSourceActor.CompletedFlow());
    }
    return this;
  }

  private Behavior<Command> onFailure(BufferSinkActor.StreamFailure failed) {
    log.error("Stream failed!", failed.cause);
    error = failed.cause;
    if (runningSearchReplyTo != null) {
      runningSearchReplyTo.tell(StatusReply.error(error));
      runningSearchLimit = 0;
      runningSearchReplyTo = null;
    }
    return this;
  }

  private Behavior<Command> onGetItems(BufferSinkActor.GetItems<S> getItems) {
    log.debug("GetItems asks for {} items, in buffer {}", getItems.amount, buffer.size());
    if (isBufferContainsEnoughItems(getItems.amount) || fetchFinished) {
      List<S> response = take(Math.min(buffer.size(), getItems.amount));
      BufferSinkActor.GetItemsResponse<S> result = new BufferSinkActor
          .GetItemsResponse<>(response, searchCompleted(), fetchFinished);
      getItems.replyTo.tell(StatusReply.success(result));
    } else if (error != null) {
      getItems.replyTo.tell(StatusReply.error(error));
    } else if (getItems.fetchWaitMode == FetchWaitMode.NO_WAIT) {
      List<S> response = take(buffer.size());
      BufferSinkActor.GetItemsResponse<S> result = new BufferSinkActor
          .GetItemsResponse<>(response, searchCompleted(), fetchFinished);
      getItems.replyTo.tell(StatusReply.success(result));
    } else {
      runningSearchLimit = getItems.amount;
      runningSearchReplyTo = getItems.replyTo;
    }
    if (error == null && !fetchFinished && getItems.amount > 0 && ackActor != null) {
      ackActor.tell(Ack.INSTANCE);
      ackActor = null;
    }
    if (error == null && searchCompleted()) {
      getItems.flowReference.tell(new DataSourceActor.CompletedFlow());
    }
    return this;
  }

  private boolean isBufferContainsEnoughItems(int requestedAmount) {
    return buffer.size() >= Math.min(bufferLimit, requestedAmount);
  }

  private boolean searchCompleted() {
    return fetchFinished && buffer.isEmpty();
  }

  private Behavior<Command> onClose(BufferSinkActor.Close cmd) {
    log.debug("Close actor ask");
    buffer.clear();
    return Behaviors.stopped();
  }

  private List<S> take(int count) {
    List<S> result = new ArrayList<>();
    for (int i = 0; i < count && !buffer.isEmpty(); i++) {
      result.add(buffer.pop());
    }
    return result;
  }

  enum Ack {
    INSTANCE
  }

  interface Command {
  }

  @Value
  static class GetItemsResponse<S> {
    List<S> items;
    boolean completed;
    boolean fetchFinished;
  }

  @Value
  static class StreamInitialized implements Command {
    ActorRef<Ack> replyTo;
  }

  @Value
  static class StreamCompleted implements Command {
    ActorRef<DataSourceActor.Command> flowReference;
  }

  static class Close implements Command {
  }

  @Value
  static class Item<S> implements Command {
    ActorRef<Ack> replyTo;
    S item;
    ActorRef<DataSourceActor.Command> flowReference;
  }

  @Value
  static class GetItems<S> implements Command {
    ActorRef<StatusReply<GetItemsResponse<S>>> replyTo;
    FetchWaitMode fetchWaitMode;
    int amount;
    ActorRef<DataSourceActor.Command> flowReference;
  }

  @Value
  static class StreamFailure implements Command {
    Throwable cause;
  }
}
