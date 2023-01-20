package com.quantori.qdp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.AskPattern;
import com.quantori.qdp.api.model.core.FetchWaitMode;
import com.quantori.qdp.api.model.core.MultiStorageSearchRequest;
import com.quantori.qdp.api.model.core.SearchItem;
import com.quantori.qdp.api.model.core.SearchResult;
import com.quantori.qdp.api.model.core.StorageItem;
import com.quantori.qdp.api.service.SearchIterator;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import lombok.extern.slf4j.Slf4j;
import scala.Tuple2;

@Slf4j
class Searcher<S extends SearchItem, I extends StorageItem> {
  private final ActorContext<SearchActor.Command> actorContext;
  private final ActorRef<BufferSinkActor.Command> bufferActorSinkRef;
  private final ActorRef<DataSourceActor.Command> flowActorRef;
  private final String user;
  private final String searchId;
  private final FetchWaitMode fetchWaitMode;

  Searcher(ActorContext<SearchActor.Command> actorContext,
           Map<String, List<SearchIterator<I>>> searchIterators,
           MultiStorageSearchRequest<S, I> multiStorageSearchRequest,
           String searchId) {
    this.actorContext = actorContext;
    this.user = multiStorageSearchRequest.getProcessingSettings().getUser();
    this.searchId = searchId;
    this.bufferActorSinkRef = actorContext.spawn(
        BufferSinkActor.create(multiStorageSearchRequest.getProcessingSettings().getBufferSize()),
        searchId + "_buffer");
    this.flowActorRef = actorContext.spawn(
        DataSourceActor.create(searchIterators, multiStorageSearchRequest, bufferActorSinkRef),
        searchId + "_flow");
    fetchWaitMode = multiStorageSearchRequest.getProcessingSettings().getFetchWaitMode();
  }

  CompletionStage<SearchResult<S>> searchNext(int limit) {
    CompletionStage<BufferSinkActor.GetItemsResponse<S>> getItemsStage = AskPattern.askWithStatus(
        bufferActorSinkRef,
        replyTo -> new BufferSinkActor.GetItems<>(replyTo, fetchWaitMode, limit, flowActorRef),
        Duration.ofMinutes(1),
        actorContext.getSystem().scheduler()
    );
    return getItemsStage
        .thenCompose(response -> AskPattern.askWithStatus(
                flowActorRef,
                DataSourceActor.StatusFlow::new,
                Duration.ofMinutes(1),
                actorContext.getSystem().scheduler()
            ).thenApply(status -> new Tuple2<>(response, status))
        ).thenApply(tuple -> {
          var response = tuple._1;
          var status = tuple._2;
          log.debug("Search next from stream result [size={}, status={}]", response.getItems().size(), status);
          return SearchResult.<S>builder()
              .searchId(searchId)
              .searchFinished(response.isCompleted())
              .countFinished(response.isFetchFinished())
              .results(response.getItems())
              .foundCount(status.getFoundByStorageCount())
              .matchedByFilterCount(status.getMatchedCount())
              .errors(status.getErrors())
              .build();
        });
  }

  void close() {
    try {
      bufferActorSinkRef.tell(new BufferSinkActor.Close());
    } catch (Exception e) {
      log.error("Cannot close a bufferActorSink", e);
    }
    try {
      flowActorRef.tell(new DataSourceActor.CloseFlow());
    } catch (Exception e) {
      log.error("Cannot close a flowActorRef", e);
    }
  }

  String getUser() {
    return user;
  }
}
