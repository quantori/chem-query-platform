package com.quantori.qdp.core.source.external;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.pattern.StatusReply;
import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import com.quantori.qdp.core.source.model.molecule.search.SearchResultItem;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BufferSinkActor extends AbstractBehavior<BufferSinkActor.Command> {

  private static final Logger logger = LoggerFactory.getLogger(BufferSinkActor.class);

  private final Deque<SearchResultItem> buffer;
  private final int bufferSize;
  private boolean completed = false;
  private Throwable error;

  private ActorRef<StatusReply<BufferSinkActor.GetItemsResponse>> runningSearchReplyTo;
  private int runningSearchLimit;
  private ActorRef<BufferSinkActor.Ack> ackActor;

  protected BufferSinkActor(ActorContext<Command> context, int bufferSize) {
    super(context);
    this.bufferSize = bufferSize;
    buffer = new ArrayDeque<>(bufferSize);
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
    ackActor = cmd.replyTo;
    cmd.replyTo.tell(BufferSinkActor.Ack.INSTANCE);

    return this;
  }

  private Behavior<Command> onItem(BufferSinkActor.Item element) {
    buffer.add(element.item);
    if (runningSearchReplyTo != null && buffer.size() >= runningSearchLimit) {
        List<SearchResultItem> items = take(runningSearchLimit);
        BufferSinkActor.GetItemsResponse response = new BufferSinkActor.GetItemsResponse(new ArrayList<>(items), false);
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
    completed = true;
    if (runningSearchReplyTo != null) {
      BufferSinkActor.GetItemsResponse response = new BufferSinkActor.GetItemsResponse(new ArrayList<>(buffer), true);
      runningSearchReplyTo.tell(StatusReply.success(response));
      buffer.clear();
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

  private Behavior<Command> onGetItems(BufferSinkActor.GetItems getItems) {
    boolean isBufferFull = buffer.size() == bufferSize;
    logger.debug("GetItems asks for {} items, in buffer {}",
            getItems.amount, buffer.size());
    if (buffer.size() >= getItems.amount || completed) {
      List<SearchResultItem> response = take(Math.min(buffer.size(), getItems.amount));
      BufferSinkActor.GetItemsResponse result = new BufferSinkActor.GetItemsResponse(response, completed && buffer.isEmpty());
      getItems.replyTo.tell(StatusReply.success(result));
    } else if (error != null) {
      getItems.replyTo.tell(StatusReply.error(error));
    } else if (getItems.waitMode == SearchRequest.WaitMode.NO_WAIT) {
      List<SearchResultItem> response = take(buffer.size());
      BufferSinkActor.GetItemsResponse result = new BufferSinkActor.GetItemsResponse(response, completed);
      getItems.replyTo.tell(StatusReply.success(result));
    } else {
      runningSearchLimit = getItems.amount;
      runningSearchReplyTo = getItems.replyTo;
    }
    if (isBufferFull && error == null) {
      ackActor.tell(BufferSinkActor.Ack.INSTANCE);
    }
    if (completed && buffer.isEmpty() && error == null) {
      getItems.flowReference.tell(new DataSourceActor.CompletedFlow());
    }
    return this;
  }

  private Behavior<Command> onClose(BufferSinkActor.Close cmd) {
    logger.debug("Close actor ask");
    buffer.clear();
    return Behaviors.stopped();
  }

  private List<SearchResultItem> take(int count) {
    List<SearchResultItem> result = new ArrayList<>();
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
  public static class GetItemsResponse {
    List<SearchResultItem> items;
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

  static class Close implements Command {
  }

  @Value
  static class Item implements Command {
    ActorRef<Ack> replyTo;
    SearchResultItem item;
    ActorRef<DataSourceActor.Command> flowReference;
  }

  @Value
  static class GetItems implements Command {
    ActorRef<StatusReply<GetItemsResponse>> replyTo;
    SearchRequest.WaitMode waitMode;
    int amount;
    ActorRef<DataSourceActor.Command> flowReference;
  }

  @Value
  static class StreamFailure implements Command {
    Throwable cause;
  }
}
