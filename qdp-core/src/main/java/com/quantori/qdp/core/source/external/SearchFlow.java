package com.quantori.qdp.core.source.external;

import akka.actor.typed.ActorRef;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.AskPattern;
import akka.pattern.StatusReply;
import com.quantori.qdp.core.source.MoleculeSearchActor;
import com.quantori.qdp.core.source.model.DataSearcher;
import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import com.quantori.qdp.core.source.model.molecule.search.SearchResult;
import java.time.Duration;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

class SearchFlow implements Searcher {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final ActorContext<MoleculeSearchActor.Command> actorContext;
  private final ActorRef<BufferSinkActor.Command> bufferActorSinkRef;
  private final ActorRef<DataSourceActor.Command> flowActorRef;
  private final SearchRequest searchRequest;
  private final String searchId;

  public SearchFlow(ActorContext<MoleculeSearchActor.Command> actorContext, DataSearcher dataSearcher,
                    SearchRequest searchRequest, String searchId) {
    this.actorContext = actorContext;
    this.searchRequest = searchRequest;
    this.searchId = searchId;
    this.bufferActorSinkRef = actorContext.spawn(
        BufferSinkActor.create(searchRequest.getProcessingSettings().getBufferSize()),
        searchId + "_buffer");
    this.flowActorRef = actorContext.spawn(
        DataSourceActor.create(dataSearcher, searchRequest, bufferActorSinkRef),
        searchId + "_flow");
  }

  public CompletionStage<SearchResult> searchNext(int limit) {
    if (limit <= 0) {
      return searchStat();
    }
    return getItems(limit).thenCompose((response) -> AskPattern.askWithStatus(
                      flowActorRef,
                      DataSourceActor.StatusFlow::new,
                      Duration.ofMinutes(1),
                      actorContext.getSystem().scheduler())
                      .thenApply(status -> new Tuple2<>(response, status)))
              .thenApply((tuple) -> {
                var response = tuple._1;
                var status = tuple._2;
                logger.debug("Search next from stream result [size={}, status={}]", response.getItems().size(), status);
                return SearchResult.builder()
                        .searchId(searchId)
                        .searchFinished(response.isCompleted())
                        .results(response.getItems())
                        .foundByStorageCount(status.getFoundByStorageCount())
                        .matchedByFilterCount(status.getMatchedCount())
                        .errorCount(status.getErrorCount())
                        .build();
              });
  }

  private CompletableFuture<BufferSinkActor.GetItemsResponse> getItems(int limit) {
    return AskPattern.askWithStatus(
                    bufferActorSinkRef,
                    (ActorRef<StatusReply<BufferSinkActor.GetItemsResponse>> replyTo) ->
                        new BufferSinkActor.GetItems(replyTo, searchRequest.getProcessingSettings().getFetchWaitMode(), limit, flowActorRef),
                    Duration.ofMinutes(1),
                    actorContext.getSystem().scheduler())
            .toCompletableFuture();
  }

  public CompletionStage<SearchResult> searchStat() {
      return AskPattern.askWithStatus(
          flowActorRef,
          DataSourceActor.StatusFlow::new,
          Duration.ofMinutes(1),
          actorContext.getSystem().scheduler())
              .thenApply((status) ->
              {
                  logger.debug("Search stat from stream: {}", status);
                  return SearchResult.builder()
                      .searchId(searchId)
                      .searchFinished(status.isCompleted())
                      .foundByStorageCount(status.getFoundByStorageCount())
                      .matchedByFilterCount(status.getMatchedCount())
                      .errorCount(status.getErrorCount())
                      .build();
              });
  }

  public void close() {
    try {
      bufferActorSinkRef.tell(new BufferSinkActor.Close());
    } catch (Exception e) {
      logger.error("Cannot close a bufferActorSink", e);
    }
    try {
      flowActorRef.tell(new DataSourceActor.CloseFlow());
    } catch (Exception e) {
      logger.error("Cannot close a flowActorRef", e);
    }
  }

  @Override
  public SearchRequest getSearchRequest() {
    return searchRequest;
  }
}
