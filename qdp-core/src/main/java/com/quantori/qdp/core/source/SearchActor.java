package com.quantori.qdp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.actor.typed.receptionist.ServiceKey;
import akka.pattern.StatusReply;
import com.quantori.qdp.core.source.model.DataSearcher;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.MultiStorageSearchRequest;
import com.quantori.qdp.core.source.model.SearchResult;
import com.quantori.qdp.core.source.model.SearchStrategy;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SearchActor extends AbstractBehavior<SearchActor.Command> {
  public static final ServiceKey<Command> searchActorsKey = ServiceKey.create(Command.class, "searchActors");

  private final String timerId = UUID.randomUUID().toString();
  private final Duration inactiveSearchTimeout = Duration.ofMinutes(5);
  private final String searchId;
  private final Map<String, DataStorage<?>> storages;
  private final Map<String, DataSearcher> dataSearchers = new HashMap<>();

  private Searcher searcher;

  public SearchActor(ActorContext<Command> context, String searchId,
                     Map<String, DataStorage<?>> storages, TimerScheduler<Command> timer) {
    super(context);
    this.searchId = searchId;
    this.storages = storages;
    // Register timer to terminate this actor in case of inactivity longer than timeout.
    timer.startSingleTimer(timerId, new SearchActor.Timeout(), inactiveSearchTimeout);
  }

  public static Behavior<Command> create(String searchId, Map<String, DataStorage<?>> storages) {
    return Behaviors.setup(ctx -> Behaviors.withTimers(timer -> new SearchActor(ctx, searchId, storages, timer)));
  }

  public static ServiceKey<Command> searchActorKey(String searchId) {
    return ServiceKey.create(Command.class, Objects.requireNonNull(searchId));
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(Search.class, this::onSearch)
        .onMessage(SearchNext.class, this::onSearchNext)
        .onMessage(Timeout.class, this::onTimeout)
        .onMessage(Close.class, this::onClose)
        .build();
  }

  private Behavior<Command> onSearch(Search searchCmd) {
    try {
      log.info("Search is started with ID {} for user: {}",
          searchId, searchCmd.multiStorageSearchRequest.getProcessingSettings().getUser());

      search(searchCmd.multiStorageSearchRequest);
      searchCmd.replyTo.tell(StatusReply.success(SearchResult.builder().searchId(searchId).results(List.of()).build()));
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

  private Behavior<Command> onSearchNext(SearchNext searchCmd) {
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
    log.info("Close command was received for search Id: {}, user {}", searchId, searcher.getUser());
    onTerminate();
    return Behaviors.stopped();
  }

  private void search(MultiStorageSearchRequest multiStorageSearchRequest) {
    log.trace("Got search initial request: {}", multiStorageSearchRequest);

    multiStorageSearchRequest.getRequestStorageMap().forEach((storageName, requestStructure) -> {
      if (storages.containsKey(storageName)) {
        dataSearchers.put(storageName, storages.get(storageName).dataSearcher(requestStructure.getStorageRequest()));
      } else {
        throw new RuntimeException(String.format("Storage %s not registered", storageName));
      }
    });

    if (multiStorageSearchRequest.getProcessingSettings().getStrategy() == SearchStrategy.PAGE_BY_PAGE) {
      searcher = new SearchByPage(dataSearchers, multiStorageSearchRequest, searchId);
    } else if (multiStorageSearchRequest.getProcessingSettings().getStrategy() == SearchStrategy.PAGE_FROM_STREAM) {
      searcher = new SearchFlow(getContext(), dataSearchers, multiStorageSearchRequest, searchId);
    } else {
      throw new UnsupportedOperationException(
          "Strategy is not implemented yet: " + multiStorageSearchRequest.getProcessingSettings().getStrategy());
    }
  }

  private CompletionStage<SearchResult> searchNext(int limit) {
    return searcher.searchNext(limit)
        .thenApply(this::prepareSearchResult);
  }

  protected void onTerminate() {
    if (searcher != null) {
      searcher.close();
    }
    for (DataSearcher dataSearcher : dataSearchers.values()) {
      try {
        dataSearcher.close();
      } catch (Exception e) {
        log.error("Failed to close data searcher " + searchId, e);
      }
    }
  }

  private SearchResult prepareSearchResult(SearchResult result) {
    return result.toBuilder()
        .resultCount(result.getMatchedByFilterCount())
        .countFinished(result.isSearchFinished())
        .build();
  }

  public interface Command {
  }

  @AllArgsConstructor
  public static class Search implements Command {
    public final ActorRef<StatusReply<SearchResult>> replyTo;
    public final MultiStorageSearchRequest multiStorageSearchRequest;
  }

  @AllArgsConstructor
  public static class SearchNext implements Command {
    public final ActorRef<StatusReply<SearchResult>> replyTo;
    public final int limit;
    public final String user;
  }

  public static class Timeout implements Command {
  }

  public static class Close implements Command {
  }
}
