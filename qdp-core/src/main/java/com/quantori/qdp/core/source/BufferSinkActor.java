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
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferSinkActor<S> extends AbstractBehavior<BufferSinkActor.Command> {

  private static final Logger logger = LoggerFactory.getLogger(BufferSinkActor.class);

  private final Deque<S> buffer;
  private final int bufferSize;
  private boolean fetchFinished = false;
  private Throwable error;

  private ActorRef<StatusReply<BufferSinkActor.GetItemsResponse<S>>> runningSearchReplyTo;
  private int runningSearchLimit;
  private ActorRef<BufferSinkActor.Ack> ackActor;

  protected BufferSinkActor(ActorContext<Command> context, int bufferSize) {
    super(context);
    this.bufferSize = bufferSize;
    buffer = new ArrayDeque<>(bufferSize);
    context.getLog().debug("Create search sink actor [bufferSize={}]", bufferSize);
  }

  public static Behavior<Command> create(int bufferSize) {
    return Behaviors.setup(ctx -> new BufferSinkActor<>(ctx, bufferSize));
  }

  public static <S> Sink<S, NotUsed> getSink(ActorRef<DataSourceActor.Command> actorRef,
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
    logger.debug("Stream initialized");
    cmd.replyTo.tell(BufferSinkActor.Ack.INSTANCE);
    return this;
  }

  private Behavior<Command> onItem(BufferSinkActor.Item<S> element) {
    buffer.add(element.item);
    if (runningSearchReplyTo != null && buffer.size() >= runningSearchLimit) {
      List<S> items = take(runningSearchLimit);
      BufferSinkActor.GetItemsResponse<S> response = new BufferSinkActor
          .GetItemsResponse<>(new ArrayList<>(items), searchCompleted(), fetchFinished);
      runningSearchReplyTo.tell(StatusReply.success(response));
      runningSearchLimit = 0;
      runningSearchReplyTo = null;
    }
    if (buffer.size() < Math.max(bufferSize, runningSearchLimit)) {
      element.replyTo.tell(BufferSinkActor.Ack.INSTANCE);
    } else {
      ackActor = element.replyTo;
    }
    return this;
  }

  private Behavior<Command> onComplete(BufferSinkActor.StreamCompleted cmd) {
    logger.debug("Stream completed");
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
    logger.error("Stream failed!", failed.cause);
    error = failed.cause;
    if (runningSearchReplyTo != null) {
      runningSearchReplyTo.tell(StatusReply.error(error));
      runningSearchLimit = 0;
      runningSearchReplyTo = null;
    }
    return this;
  }

  private Behavior<Command> onGetItems(BufferSinkActor.GetItems<S> getItems) {
    logger.debug("GetItems asks for {} items, in buffer {}", getItems.amount, buffer.size());
    if (buffer.size() >= getItems.amount || fetchFinished) {
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

  private boolean searchCompleted() {
    return fetchFinished && buffer.isEmpty();
  }

  private Behavior<Command> onClose(BufferSinkActor.Close cmd) {
    logger.debug("Close actor ask");
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

  public enum Ack {
    INSTANCE
  }

  public interface Command {
  }

  @Value
  public static class GetItemsResponse<S> {
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

  public static class Close implements Command {
  }

  @Value
  static class Item<S> implements Command {
    ActorRef<Ack> replyTo;
    S item;
    ActorRef<DataSourceActor.Command> flowReference;
  }

  @Value
  public static class GetItems<S> implements Command {
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
