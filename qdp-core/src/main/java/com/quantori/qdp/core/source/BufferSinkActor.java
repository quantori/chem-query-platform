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
import com.quantori.qdp.core.model.FetchWaitMode;
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
  private final boolean isCountTask;
  private final AtomicInteger fetchCounter = new AtomicInteger(0);
  private boolean fetchFinished = false;
  private Throwable error;

  private ActorRef<StatusReply<BufferSinkActor.GetItemsResponse<S>>> runningSearchReplyTo;
  private int runningSearchLimit;
  private ActorRef<BufferSinkActor.Ack> ackActor;

  BufferSinkActor(
      ActorContext<Command> context, int bufferLimit, int fetchLimit, boolean isCountTask) {
    super(context);
    this.bufferLimit = bufferLimit;
    this.fetchLimit = fetchLimit;
    this.isCountTask = isCountTask;
    this.buffer = new ArrayDeque<>(bufferLimit);
    log.debug("Create search sink actor [bufferSize={}]", bufferLimit);
  }

  static Behavior<Command> create(int bufferSize, int fetchLimit, boolean isCountTask) {
    return Behaviors.setup(ctx -> new BufferSinkActor<>(ctx, bufferSize, fetchLimit, isCountTask));
  }

  static <S> Sink<S, NotUsed> getSink(
      ActorRef<DataSourceActor.Command> actorRef,
      ActorRef<BufferSinkActor.Command> bufferActorSinkRef) {
    return ActorSink.actorRefWithBackpressure(
        bufferActorSinkRef,
        (replyTo, item) -> new BufferSinkActor.Item<>(replyTo, item, actorRef),
        BufferSinkActor.StreamInitialized::new,
        BufferSinkActor.Ack.INSTANCE,
        new BufferSinkActor.StreamCompleted(actorRef),
        BufferSinkActor.StreamFailure::new);
  }

  @Override
  public Receive<Command> createReceive() {
    ReceiveBuilder<Command> builder = newReceiveBuilder();
    builder.onMessage(BufferSinkActor.StreamInitialized.class, this::onInitFlow);
    builder.onMessage(BufferSinkActor.Item.class, this::onItem);
    builder.onMessage(GetSearchResult.class, this::onGetSearchResult);
    builder.onMessage(GetCountResult.class, this::onGetCountResult);
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
    fetchCounter.incrementAndGet();
    return isCountTask ? onCountItem(element) : onSearchItem(element);
  }

  private Behavior<Command> onSearchItem(BufferSinkActor.Item<S> element) {
    buffer.add(element.item);

    if (runningSearchReplyTo != null && isBufferContainsEnoughItems(runningSearchLimit)) {
      List<S> items = take(runningSearchLimit);
      BufferSinkActor.GetItemsResponse<S> response =
          new BufferSinkActor.GetItemsResponse<>(
              new ArrayList<>(items), searchCompleted(), fetchFinished);
      runningSearchReplyTo.tell(StatusReply.success(response));
      runningSearchLimit = 0;
      runningSearchReplyTo = null;
    }

    if (fetchLimit > 0) {
      if (fetchCounter.get() == fetchLimit) {
        log.debug("Stream fetch limit reached");
        ackActor = element.replyTo;
        complete(element.flowReference);
        return this;
      }
    }

    if (buffer.size() < this.bufferLimit) {
      // keep reading elements till we fill the buffer
      element.replyTo.tell(BufferSinkActor.Ack.INSTANCE);
    } else {
      // suspend fetching more elements
      ackActor = element.replyTo;
    }
    return this;
  }

  private Behavior<Command> onCountItem(BufferSinkActor.Item<S> element) {
    if (fetchLimit > 0) {
      if (fetchCounter.get() == fetchLimit) {
        log.debug("Stream fetch limit reached");
        ackActor = element.replyTo;
        complete(element.flowReference);
        return this;
      }
    }
    element.replyTo.tell(BufferSinkActor.Ack.INSTANCE);
    return this;
  }

  private Behavior<Command> onComplete(BufferSinkActor.StreamCompleted cmd) {
    log.debug("Stream completed");
    complete(cmd.flowReference);
    return this;
  }

  private void complete(ActorRef<DataSourceActor.Command> ref) {
    fetchFinished = true;
    if (runningSearchReplyTo != null) {
      List<S> response = take(Math.min(buffer.size(), runningSearchLimit));
      GetItemsResponse<S> result =
          new GetItemsResponse<>(response, searchCompleted(), fetchFinished);
      runningSearchReplyTo.tell(StatusReply.success(result));
      runningSearchLimit = 0;
      runningSearchReplyTo = null;
    }
    if (buffer.isEmpty()) {
      ref.tell(new DataSourceActor.CompletedFlow());
    }
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

  private Behavior<Command> onGetCountResult(GetCountResult<S> getSearchResults) {
    log.debug("GetSearchResult asks for counter value");
    if (error != null) {
      getSearchResults.replyTo.tell(StatusReply.error(error));
    }

    GetCountResponse result = new GetCountResponse(fetchCounter.get(), fetchFinished);
    getSearchResults.replyTo.tell(StatusReply.success(result));

    if (error == null && !fetchFinished && ackActor != null) {
      ackActor.tell(Ack.INSTANCE);
      ackActor = null;
    }
    if (error == null && fetchFinished) {
      getSearchResults.flowReference.tell(new DataSourceActor.CompletedFlow());
    }
    return this;
  }

  private Behavior<Command> onGetSearchResult(GetSearchResult<S> getSearchResult) {
    log.debug(
        "GetSearchResult asks for {} items, in buffer {}", getSearchResult.amount, buffer.size());
    if (isBufferContainsEnoughItems(getSearchResult.amount) || fetchFinished) {
      List<S> response = take(Math.min(buffer.size(), getSearchResult.amount));
      BufferSinkActor.GetItemsResponse<S> result =
          new BufferSinkActor.GetItemsResponse<>(response, searchCompleted(), fetchFinished);
      getSearchResult.replyTo.tell(StatusReply.success(result));
    } else if (error != null) {
      getSearchResult.replyTo.tell(StatusReply.error(error));
    } else if (getSearchResult.fetchWaitMode == FetchWaitMode.NO_WAIT) {
      List<S> response = take(buffer.size());
      BufferSinkActor.GetItemsResponse<S> result =
          new BufferSinkActor.GetItemsResponse<>(response, searchCompleted(), fetchFinished);
      getSearchResult.replyTo.tell(StatusReply.success(result));
    } else {
      runningSearchLimit = getSearchResult.amount;
      runningSearchReplyTo = getSearchResult.replyTo;
    }
    if (error == null && !fetchFinished && getSearchResult.amount > 0 && ackActor != null) {
      ackActor.tell(Ack.INSTANCE);
      ackActor = null;
    }
    if (error == null && searchCompleted()) {
      getSearchResult.flowReference.tell(new DataSourceActor.CompletedFlow());
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

  interface Command {}

  @Value
  static class GetItemsResponse<S> {
    List<S> items;
    boolean completed;
    boolean fetchFinished;
  }

  @Value
  static class GetCountResponse {
    long count;
    boolean completed;
  }

  @Value
  static class StreamInitialized implements Command {
    ActorRef<Ack> replyTo;
  }

  @Value
  static class StreamCompleted implements Command {
    ActorRef<DataSourceActor.Command> flowReference;
  }

  static class Close implements Command {}

  @Value
  static class Item<S> implements Command {
    ActorRef<Ack> replyTo;
    S item;
    ActorRef<DataSourceActor.Command> flowReference;
  }

  @Value
  static class GetSearchResult<S> implements Command {
    ActorRef<StatusReply<GetItemsResponse<S>>> replyTo;
    FetchWaitMode fetchWaitMode;
    int amount;
    ActorRef<DataSourceActor.Command> flowReference;
  }

  @Value
  static class GetCountResult<S> implements Command {
    ActorRef<StatusReply<GetCountResponse>> replyTo;
    ActorRef<DataSourceActor.Command> flowReference;
  }

  @Value
  static class StreamFailure implements Command {
    Throwable cause;
  }
}
