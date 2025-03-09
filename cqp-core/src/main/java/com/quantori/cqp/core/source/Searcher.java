package com.quantori.cqp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.AskPattern;
import com.quantori.cqp.core.model.FetchWaitMode;
import com.quantori.cqp.core.model.SearchItem;
import com.quantori.cqp.core.model.SearchIterator;
import com.quantori.cqp.core.model.SearchRequest;
import com.quantori.cqp.core.model.SearchResult;
import com.quantori.cqp.core.model.StorageItem;
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

  Searcher(
      ActorContext<SearchActor.Command> actorContext,
      Map<String, List<SearchIterator<I>>> searchIterators,
      SearchRequest<S, I> searchRequest,
      String searchId) {
    this.actorContext = actorContext;
    this.user = searchRequest.getUser();
    this.searchId = searchId;
    this.bufferActorSinkRef =
        actorContext.spawn(
            BufferSinkActor.create(
                searchRequest.getBufferSize(),
                searchRequest.getFetchLimit(),
                searchRequest.isCountTask()),
            searchId + "_buffer");
    this.flowActorRef =
        actorContext.spawn(
            DataSourceActor.create(searchIterators, searchRequest, bufferActorSinkRef),
            searchId + "_flow");
    fetchWaitMode = searchRequest.getFetchWaitMode();
  }

  CompletionStage<SearchResult<S>> getSearchResult(int limit) {
    CompletionStage<BufferSinkActor.GetItemsResponse<S>> getItemsStage =
        AskPattern.askWithStatus(
            bufferActorSinkRef,
            replyTo ->
                new BufferSinkActor.GetSearchResult<>(replyTo, fetchWaitMode, limit, flowActorRef),
            Duration.ofMinutes(1),
            actorContext.getSystem().scheduler());
    return getItemsStage
        .thenCompose(
            response ->
                AskPattern.askWithStatus(
                        flowActorRef,
                        DataSourceActor.StatusFlow::new,
                        Duration.ofMinutes(1),
                        actorContext.getSystem().scheduler())
                    .thenApply(status -> new Tuple2<>(response, status)))
        .thenApply(
            tuple -> {
              var response = tuple._1;
              var status = tuple._2;
              log.debug(
                  "Get next results from the buffer, result [size={}, status={}]",
                  response.getItems().size(),
                  status);
              return SearchResult.<S>builder()
                  .searchId(searchId)
                  .searchFinished(response.isCompleted())
                  .results(response.getItems())
                  .foundCount(status.getFoundByStorageCount())
                  .matchedByFilterCount(status.getMatchedCount())
                  .errors(status.getErrors())
                  .build();
            });
  }

  CompletionStage<SearchResult<S>> getCounterResult() {
    CompletionStage<BufferSinkActor.GetCountResponse> getCounterStage =
        AskPattern.askWithStatus(
            bufferActorSinkRef,
            replyTo -> new BufferSinkActor.GetCountResult<>(replyTo, flowActorRef),
            Duration.ofMinutes(1),
            actorContext.getSystem().scheduler());
    return getCounterStage
        .thenCompose(
            response ->
                AskPattern.askWithStatus(
                        flowActorRef,
                        DataSourceActor.StatusFlow::new,
                        Duration.ofMinutes(1),
                        actorContext.getSystem().scheduler())
                    .thenApply(status -> new Tuple2<>(response, status)))
        .thenApply(
            tuple -> {
              var response = tuple._1;
              var status = tuple._2;
              log.debug(
                  "Get next counter results from the buffer counter, result[counter={}, status={}]",
                  response.getCount(),
                  status);
              return SearchResult.<S>builder()
                  .searchId(searchId)
                  .searchFinished(response.isCompleted())
                  .resultCount(response.getCount())
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
