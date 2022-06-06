package com.quantori.qdp.core.source.external;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.TimerScheduler;
import com.quantori.qdp.core.source.MoleculeSearchActor;
import com.quantori.qdp.core.source.model.DataSearcher;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.molecule.Molecule;
import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import com.quantori.qdp.core.source.model.molecule.search.SearchResult;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class ExternalSearchActor extends MoleculeSearchActor {

  private final DataStorage<? extends Molecule> storage;
  private DataSearcher dataSearcher;
  private final AtomicLong countTaskResult = new AtomicLong();
  private Future<?> countTask;

  private Searcher searcher;

  public static Behavior<Command> create(String storageName, DataStorage<? extends Molecule> storage) {
    return Behaviors.setup(ctx ->
        Behaviors.withTimers(timer -> new ExternalSearchActor(ctx, storageName, storage, timer)));
  }

  private ExternalSearchActor(ActorContext<Command> context, String storageName,
                              DataStorage<? extends Molecule> storage, TimerScheduler<Command> timerScheduler) {
    super(context, storageName, timerScheduler);
    this.storage = storage;
  }

  @Override
  protected CompletionStage<SearchResult> search(SearchRequest searchRequest) {
    getContext().getLog().trace("Got search initial request: {}", searchRequest);

    dataSearcher = storage.dataSearcher(searchRequest);
    //TODO: run this task conditionally?
    countTask = runCountSearch(searchRequest);

    if (searchRequest.getStrategy() == SearchRequest.SearchStrategy.PAGE_BY_PAGE) {
      searcher = new SearchByPage(searchRequest, dataSearcher, searchId);
    } else if (searchRequest.getStrategy() == SearchRequest.SearchStrategy.PAGE_FROM_STREAM) {
      searcher = new SearchFlow(getContext(), dataSearcher, searchRequest, searchId);
    } else {
      throw new UnsupportedOperationException("Strategy is not implemented yet: " + searchRequest.getStrategy());
    }

    return searcher.searchNext(searchRequest.getPageSize())
            .thenApply(result -> result.copyBuilder().resultCount(countTaskResult.get()).countFinished(countTask.isDone()).build());
  }

  @Override
  protected CompletionStage<SearchResult> searchStatistics() {
    return searcher.searchStat().thenApply(result -> result.copyBuilder().resultCount(countTaskResult.get()).countFinished(countTask.isDone()).build());
  }

  @Override
  protected SearchRequest getSearchRequest() {
    return searcher.getSearchRequest();
  }

  @Override
  protected CompletionStage<SearchResult> searchNext(int limit) {
    return searcher.searchNext(limit).thenApply(result -> result.copyBuilder().resultCount(countTaskResult.get()).countFinished(countTask.isDone()).build());
  }

  @Override
  protected void onTerminate() {
    if (countTask != null) {
      countTask.cancel(true);
    }
    if (searcher != null) {
      searcher.close();
    }
    try {
      dataSearcher.close();
    } catch (Exception e) {
      getContext().getLog().error("Failed to close data searcher", e);
    }
  }

  private Future<?> runCountSearch(SearchRequest searchRequest) {
    return getExecutor().submit(() -> {
      List<? extends SearchRequest.StorageResultItem> storageResultItems;
      try (DataSearcher dataSearcher = storage.dataSearcher(searchRequest)) {

        while ((storageResultItems = dataSearcher.next()).size() > 0) {

          long count = storageResultItems.stream()
              .filter(res -> searchRequest.getResultFilter().test(res))
              .count();

          countTaskResult.addAndGet(count);

          if (Thread.interrupted()) {
            break;
          }
        }
      } catch (Exception e) {
        getContext().getLog().error("Error when open data searcher", e);
      }
    });
  }
}
