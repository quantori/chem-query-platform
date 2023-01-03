package com.quantori.qdp.core.source;

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
import com.quantori.qdp.api.model.core.DataSearcher;
import com.quantori.qdp.api.model.core.DataStorage;
import com.quantori.qdp.api.model.core.MultiStorageSearchRequest;
import com.quantori.qdp.api.model.core.RequestStructure;
import com.quantori.qdp.api.model.core.SearchItem;
import com.quantori.qdp.api.model.core.SearchResult;
import com.quantori.qdp.api.model.core.StorageItem;
import com.quantori.qdp.api.model.core.StorageRequest;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SearchActor<S extends SearchItem> extends AbstractBehavior<SearchActor.Command> {
  public static final ServiceKey<Command> searchActorsKey = ServiceKey.create(Command.class, "searchActors");
  private final ExecutorService executor = Executors.newCachedThreadPool();

  private final String timerId = UUID.randomUUID().toString();
  private final Duration inactiveSearchTimeout = Duration.ofMinutes(5);
  private final String searchId;
  private final Map<String, DataStorage<?>> storages;
  private MultiStorageSearchRequest<S> multiStorageSearchRequest;
  private final Map<String, List<DataSearcher>> dataSearchers = new HashMap<>();
  private final AtomicLong countTaskResult = new AtomicLong();
  private Future<?> countTask;

  private Searcher<S> searcher;

  public SearchActor(ActorContext<Command> context, String searchId,
                     Map<String, DataStorage<?>> storages, TimerScheduler<Command> timer) {
    super(context);
    this.searchId = searchId;
    this.storages = storages;
    // Register timer to terminate this actor in case of inactivity longer than timeout.
    timer.startSingleTimer(timerId, new SearchActor.Timeout(), inactiveSearchTimeout);
  }

  public static Behavior<Command> create(String searchId, Map<String, DataStorage<?>> storages) {
    return Behaviors.setup(ctx -> Behaviors.withTimers(timer -> new SearchActor<>(ctx, searchId, storages, timer)));
  }

  public static ServiceKey<Command> searchActorKey(String searchId) {
    return ServiceKey.create(Command.class, Objects.requireNonNull(searchId));
  }

  @Override
  public Receive<Command> createReceive() {
    ReceiveBuilder<Command> builder = newReceiveBuilder();
    builder.onMessage(Search.class, this::onSearch);
    builder.onMessage(SearchNext.class, this::onSearchNext);
    builder.onMessage(GetSearchRequest.class, this::onGetSearchRequest);
    builder.onMessage(Timeout.class, this::onTimeout);
    builder.onMessage(Close.class, this::onClose);
    return builder.build();
  }

  private Behavior<Command> onGetSearchRequest(GetSearchRequest cmd) {
    if (!cmd.user.equals(multiStorageSearchRequest.getProcessingSettings().getUser())) {
      cmd.replyTo.tell(StatusReply.error("Search request access violation by user " + cmd.user));
    }
    cmd.replyTo.tell(StatusReply.success(multiStorageSearchRequest.getRequestStorageMap()
        .get(cmd.storage).getStorageRequest()));

    return Behaviors.withTimers(timer -> {
      timer.startSingleTimer(timerId, new Timeout(), inactiveSearchTimeout);
      return this;
    });
  }

  private Behavior<Command> onSearch(Search<S> searchCmd) {
    try {
      this.multiStorageSearchRequest = searchCmd.multiStorageSearchRequest;
      log.info("Search is started with ID {} for user: {}",
          searchId, searchCmd.multiStorageSearchRequest.getProcessingSettings().getUser());
      if (searchCmd.multiStorageSearchRequest.getProcessingSettings().isRunCountTask()) {
        countTask = runCountSearch(searchCmd);
      }
      search(searchCmd.multiStorageSearchRequest);
      searchCmd.replyTo.tell(
          StatusReply.success(SearchResult.<S>builder().searchId(searchId).results(List.of()).build()));
    } catch (Exception ex) {
      log.error(String.format("Molecule search failed: %s with ID %s for user: %s",
          searchCmd.multiStorageSearchRequest, searchId,
          searchCmd.multiStorageSearchRequest.getProcessingSettings().getUser()), ex);
      searchCmd.replyTo.tell(StatusReply.error(ex));
    }

    return Behaviors.withTimers(timer -> {
      timer.startSingleTimer(timerId, new Timeout(), inactiveSearchTimeout);
      return this;
    });
  }

  private Behavior<Command> onSearchNext(SearchNext<S> searchCmd) {
    if (!searchCmd.user.equals(searcher.getUser())) {
      searchCmd.replyTo.tell(StatusReply.error("Search result access violation by user " + searchCmd.user));
    }

    searchNext(searchCmd.limit).whenComplete((result, error) -> {
      if (error == null) {
        searchCmd.replyTo.tell(StatusReply.success(result));
      } else {
        searchCmd.replyTo.tell(StatusReply.error(error.getMessage()));
        log.error(String.format("Molecule search next failed with ID %s for user: %s", searchId,
            searcher.getUser()), error);
      }
    });

    return Behaviors.withTimers(timer -> {
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

  private void search(MultiStorageSearchRequest<S> multiStorageSearchRequest) {
    log.trace("Got search initial request: {}", multiStorageSearchRequest);
    this.multiStorageSearchRequest = multiStorageSearchRequest;
    multiStorageSearchRequest.getRequestStorageMap().forEach((storageName, requestStructure) -> {
      if (storages.containsKey(storageName)) {
        dataSearchers.put(storageName, storages.get(storageName).dataSearcher(requestStructure));
      } else {
        throw new RuntimeException(String.format("Storage %s not registered", storageName));
      }
    });
    searcher = new SearchFlow<>(getContext(), dataSearchers, multiStorageSearchRequest, searchId);
  }

  private CompletionStage<SearchResult<S>> searchNext(int limit) {
    return searcher.searchNext(limit)
        .thenApply(this::prepareSearchResult);
  }

  protected void onTerminate() {
    if (countTask != null) {
      countTask.cancel(true);
    }
    if (searcher != null) {
      searcher.close();
    }
    for (DataSearcher dataSearcher : dataSearchers.values().stream().flatMap(Collection::stream).toList()) {
      try {
        dataSearcher.close();
      } catch (Exception e) {
        log.error("Failed to close data searcher " + searchId, e);
      }
    }
  }

  private SearchResult<S> prepareSearchResult(SearchResult<S> result) {
    if (multiStorageSearchRequest.getProcessingSettings().isRunCountTask()) {
      if (result.isSearchFinished()) {
        return result.toBuilder().resultCount(result.getMatchedByFilterCount()).countFinished(true).build();
      }
      return result.toBuilder().resultCount(countTaskResult.get()).countFinished(countTask.isDone()).build();
    } else {
      return result.toBuilder()
          .resultCount(result.getMatchedByFilterCount())
          .build();
    }
  }

  private Future<?> runCountSearch(Search<?> searchRequest) {
    return executor.submit(() -> multiStorageSearchRequest.getRequestStorageMap().entrySet().stream()
        .filter(entry -> storages.containsKey(entry.getKey()))
        .forEach(entry -> runCountByStorage(searchRequest, entry.getKey(), entry.getValue())));
  }

  private void runCountByStorage(Search<?> searchRequest, String storageName, RequestStructure<S> requestStructure) {
    for (DataSearcher dSearcher : storages.get(storageName).dataSearcher(requestStructure)) {
      try (DataSearcher dataSearcher = dSearcher) {
        List<? extends StorageItem> storageResultItems;
        while (!(storageResultItems = dataSearcher.next()).isEmpty()) {
          long count = storageResultItems.stream()
              .filter(requestStructure.getResultFilter())
              .count();
          countTaskResult.addAndGet(count);
          if (Thread.interrupted()) {
            break;
          }
        }
      } catch (Exception e) {
        getContext().getLog().error(String.format("Error in search counter for id:%s, user %s", searchId,
            searchRequest.multiStorageSearchRequest.getProcessingSettings().getUser()), e);
      }
    }
  }

  public interface Command {
  }

  @AllArgsConstructor
  public static class Search<S extends SearchItem> implements Command {
    public final ActorRef<StatusReply<SearchResult<S>>> replyTo;
    public final MultiStorageSearchRequest<S> multiStorageSearchRequest;
  }

  @AllArgsConstructor
  public static class SearchNext<S extends SearchItem> implements Command {
    public final ActorRef<StatusReply<SearchResult<S>>> replyTo;
    public final int limit;
    public final String user;
  }

  @AllArgsConstructor
  public static class GetSearchRequest implements Command {
    public final ActorRef<StatusReply<StorageRequest>> replyTo;
    public final String storage;
    public final String user;
  }

  public static class Timeout implements Command {
  }

  @AllArgsConstructor
  public static class Close implements Command {
    public final String user;
  }
}
