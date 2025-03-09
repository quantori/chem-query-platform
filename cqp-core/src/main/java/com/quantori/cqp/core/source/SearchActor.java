package com.quantori.cqp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.ReceiveBuilder;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.actor.typed.receptionist.ServiceKey;
import akka.pattern.StatusReply;
import com.quantori.cqp.core.model.DataStorage;
import com.quantori.cqp.core.model.SearchItem;
import com.quantori.cqp.core.model.SearchIterator;
import com.quantori.cqp.core.model.SearchRequest;
import com.quantori.cqp.core.model.SearchResult;
import com.quantori.cqp.core.model.StorageItem;
import com.quantori.cqp.core.model.StorageRequest;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class SearchActor<S extends SearchItem, I extends StorageItem>
    extends AbstractBehavior<SearchActor.Command> {
  static final ServiceKey<Command> searchActorsKey =
      ServiceKey.create(Command.class, "searchActors");

  private final String timerId = UUID.randomUUID().toString();
  private final Duration inactiveSearchTimeout = Duration.ofMinutes(5);
  private final String searchId;
  private final Map<String, DataStorage<?, I>> storages;
  private final Map<String, List<SearchIterator<I>>> searchIterators = new HashMap<>();
  private SearchRequest<S, I> searchRequest;
  private Searcher<S, I> searcher;

  SearchActor(
      ActorContext<Command> context,
      String searchId,
      Map<String, DataStorage<?, I>> storages,
      TimerScheduler<Command> timer) {
    super(context);
    this.searchId = searchId;
    this.storages = storages;
    // Register timer to terminate this actor in case of inactivity longer than timeout.
    timer.startSingleTimer(timerId, new SearchActor.Timeout(), inactiveSearchTimeout);
  }

  static <I extends StorageItem> Behavior<Command> create(
      String searchId, Map<String, DataStorage<?, I>> storages) {
    return Behaviors.setup(
        ctx -> Behaviors.withTimers(timer -> new SearchActor<>(ctx, searchId, storages, timer)));
  }

  static ServiceKey<Command> searchActorKey(String searchId) {
    return ServiceKey.create(Command.class, Objects.requireNonNull(searchId));
  }

  @Override
  public Receive<Command> createReceive() {
    ReceiveBuilder<Command> builder = newReceiveBuilder();
    builder.onMessage(Search.class, this::onSearch);
    builder.onMessage(GetNextSearchResult.class, this::onGetNextSearchResult);
    builder.onMessage(GetStorageRequest.class, this::onGetSearchRequest);
    builder.onMessage(Timeout.class, this::onTimeout);
    builder.onMessage(Close.class, this::onClose);
    return builder.build();
  }

  private Behavior<Command> onGetSearchRequest(GetStorageRequest cmd) {
    if (!cmd.user.equals(searchRequest.getUser())) {
      cmd.replyTo.tell(StatusReply.error("Search request access violation by user " + cmd.user));
    }
    cmd.replyTo.tell(StatusReply.success(searchRequest.getRequestStorageMap().get(cmd.storage)));

    return Behaviors.withTimers(
        timer -> {
          timer.startSingleTimer(timerId, new Timeout(), inactiveSearchTimeout);
          return this;
        });
  }

  private Behavior<Command> onSearch(Search<S, I> searchCmd) {
    try {
      this.searchRequest = searchCmd.searchRequest;
      log.info(
          "Search is started with ID {} for user: {}", searchId, searchCmd.searchRequest.getUser());
      search(searchCmd.searchRequest);
      SearchResult<S> searchResult =
          SearchResult.<S>builder().searchId(searchId).results(List.of()).build();
      searchCmd.replyTo.tell(StatusReply.success(searchResult));
    } catch (Exception ex) {
      log.error(
          "Molecule search failed: {} with ID {} for user: {}",
          searchCmd.searchRequest,
          searchId,
          searchCmd.searchRequest.getUser(),
          ex);
      searchCmd.replyTo.tell(StatusReply.error(ex));
    }

    return Behaviors.withTimers(
        timer -> {
          timer.startSingleTimer(timerId, new Timeout(), inactiveSearchTimeout);
          return this;
        });
  }

  private Behavior<Command> onGetNextSearchResult(GetNextSearchResult<S> getResultCmd) {
    if (!getResultCmd.user.equals(searcher.getUser())) {
      getResultCmd.replyTo.tell(
          StatusReply.error("Search result access violation by user " + getResultCmd.user));
    }

    CompletionStage<SearchResult<S>> searcherResult =
        searchRequest.isCountTask()
            ? searcher.getCounterResult()
            : searcher.getSearchResult(getResultCmd.limit);

    searcherResult.whenComplete(
        (SearchResult<S> result, Throwable error) -> {
          if (error == null) {
            getResultCmd.replyTo.tell(StatusReply.success(result));
          } else {
            getResultCmd.replyTo.tell(StatusReply.error(error.getMessage()));
            log.error(
                "Molecule search next failed with ID {} for user: {}",
                searchId,
                searcher.getUser(),
                error);
          }
        });

    return Behaviors.withTimers(
        timer -> {
          timer.startSingleTimer(timerId, new Timeout(), inactiveSearchTimeout);
          return this;
        });
  }

  private Behavior<Command> onTimeout(Timeout cmd) {
    log.debug("Reached search actor timeout (will stop actor): " + getContext().getSelf());
    onTerminate();
    return Behaviors.stopped();
  }

  private Behavior<Command> onClose(Close cmd) {
    if (cmd.user != null && !cmd.user.equals(searcher.getUser())) {
      log.error("Search access violation by user " + cmd.user);
      return this;
    }
    log.info("Close command was received for search Id: {}, user {}", searchId, searcher.getUser());
    onTerminate();
    return Behaviors.stopped();
  }

  private void search(SearchRequest<S, I> searchRequest) {
    log.trace("Got search initial request: {}", searchRequest);
    this.searchRequest = searchRequest;

    searchRequest
        .getRequestStorageMap()
        .forEach(
            (storageName, requestStructure) -> {
              if (storages.containsKey(storageName)) {
                searchIterators.put(
                    storageName, storages.get(storageName).searchIterator(requestStructure));
              } else {
                throw new RuntimeException(String.format("Storage %s not registered", storageName));
              }
            });
    searcher = new Searcher<>(getContext(), searchIterators, searchRequest, searchId);
  }

  void onTerminate() {
    if (searcher != null) {
      searcher.close();
    }
    for (SearchIterator<I> searchIterator :
        searchIterators.values().stream().flatMap(Collection::stream).toList()) {
      try {
        searchIterator.close();
      } catch (Exception e) {
        log.error("Failed to close data searcher " + searchId, e);
      }
    }
  }

  interface Command {}

  @AllArgsConstructor
  static class Search<S extends SearchItem, I extends StorageItem> implements Command {
    public final ActorRef<StatusReply<SearchResult<S>>> replyTo;
    public final SearchRequest<S, I> searchRequest;
  }

  @AllArgsConstructor
  static class GetNextSearchResult<S extends SearchItem> implements Command {
    public final ActorRef<StatusReply<SearchResult<S>>> replyTo;
    public final int limit;
    public final String user;
  }

  @AllArgsConstructor
  static class GetStorageRequest implements Command {
    public final ActorRef<StatusReply<StorageRequest>> replyTo;
    public final String storage;
    public final String user;
  }

  static class Timeout implements Command {}

  @AllArgsConstructor
  static class Close implements Command {
    public final String user;
  }
}
