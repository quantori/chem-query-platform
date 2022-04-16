package com.quantori.qdp.core.source.external;

import akka.actor.typed.ActorRef;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.AskPattern;
import com.quantori.qdp.core.source.MoleculeSearchActor;
import com.quantori.qdp.core.source.model.DataSearcher;
import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import com.quantori.qdp.core.source.model.molecule.search.SearchResult;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        BufferSinkActor.create(searchRequest.getBufferSize()),
        searchId + "_buffer");
    this.flowActorRef = actorContext.spawn(
        DataSourceActor.create(dataSearcher, searchRequest, bufferActorSinkRef),
        searchId + "_flow");
  }

  public SearchResult searchNext(int limit) {
    if (limit <= 0) {
      searchStat();
    }
    try {
      CompletionStage<BufferSinkActor.GetItemsResponse> responseStage = AskPattern.askWithStatus(
          bufferActorSinkRef,
          replyTo -> new BufferSinkActor.GetItems(replyTo, limit),
          Duration.ofMinutes(1),
          actorContext.getSystem().scheduler());
      CompletionStage<DataSourceActor.StatusResponse> statusStage = AskPattern.askWithStatus(
          flowActorRef,
          DataSourceActor.StatusFlow::new,
          Duration.ofMinutes(1),
          actorContext.getSystem().scheduler());
      return responseStage.thenCombine(
              statusStage,
              (bufferItems, status) -> toSearchResult(limit, bufferItems, status)
          ).toCompletableFuture().get();
    } catch (InterruptedException | ExecutionException e) {
      logger.error("Failed to get result from buffer actor", e);
      Thread.currentThread().interrupt();
      return SearchResult.builder().searchId(searchId).searchFinished(true).build();
    }
  }

  public SearchResult searchStat() {
    try {
      CompletionStage<DataSourceActor.StatusResponse> statusStage = AskPattern.askWithStatus(
          flowActorRef,
          DataSourceActor.StatusFlow::new,
          Duration.ofMinutes(1),
          actorContext.getSystem().scheduler());
      DataSourceActor.StatusResponse status = statusStage.toCompletableFuture().get();

      logger.debug("Search stat from stream: {}", status);

      //TODO: .searchFinished(false) in sdf, sdf tests failed if status.isCompleted()
      return SearchResult.builder()
          .searchId(searchId)
          .searchFinished(false)
          .foundByStorageCount(status.getFoundByStorageCount())
          .matchedByFilterCount(status.getMatchedCount())
          .errorCount(status.getErrorCount())
          .build();
    } catch (InterruptedException | ExecutionException e) {
      logger.error("Failed to get search stat from buffer actor", e);
      Thread.currentThread().interrupt();
      return SearchResult.builder().searchId(searchId).searchFinished(true).build();
    }
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

  private SearchResult toSearchResult(
      int limit,
      BufferSinkActor.GetItemsResponse bufferItems,
      DataSourceActor.StatusResponse status) {
    if (bufferItems.getBufferSize() < limit && !status.isCompleted() && status.isPaused()) {
      logger.debug("Ready to resume the flow with current collection size: {} and flowIsPaused: {} "
              + "and completed: {}, buffer size: {}",
          bufferItems.getItems().size(), status.isPaused(), status.isCompleted(), bufferItems.getBufferSize());
      resumeFlow();
    }

    logger.debug("Search next from stream result [size={}, status={}]", bufferItems.getItems().size(), status);

    //TODO: bufferItems.getBufferSize() == 0 from sdf
    return SearchResult.builder()
        .searchId(searchId)
        .searchFinished(status.isCompleted() && bufferItems.getBufferSize() == 0)
        .results(bufferItems.getItems())
        .foundByStorageCount(status.getFoundByStorageCount())
        .matchedByFilterCount(status.getMatchedCount())
        .errorCount(status.getErrorCount())
        .build();
  }

  private void resumeFlow() {
    flowActorRef.tell(new DataSourceActor.StartFlow());
  }
}
