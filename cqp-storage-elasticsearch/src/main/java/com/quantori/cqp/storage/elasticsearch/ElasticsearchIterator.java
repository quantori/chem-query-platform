package com.quantori.cqp.storage.elasticsearch;

import static com.quantori.cqp.storage.elasticsearch.ElasticsearchStorageConfiguration.STORAGE_TYPE;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.ResponseBody;
import co.elastic.clients.transport.ElasticsearchTransport;
import com.quantori.cqp.api.model.Flattened;
import com.quantori.cqp.core.model.SearchIterator;
import com.quantori.cqp.storage.elasticsearch.model.MoleculeDocument;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Elastic adopted search result set that uses scroll identifiers. Implementation must provide type of result generated
 * by this search result set and conversion method from {@link Hit} to result type.
 *
 * @see Molecules
 * @see Reactions
 */
@Slf4j
abstract class ElasticsearchIterator<TDocument, T> implements SearchIterator<T> {

  private final Time timeout;
  private final ElasticsearchClient client;
  private final ElasticsearchAsyncClient asyncClient;
  private final Function<List<Hit<TDocument>>, List<T>> converter;
  private final SearchRequest searchRequest;
  private final AtomicReference<String> scrollIdRef;
  @Getter(onMethod = @__({@Override}))
  private final String storageName;
  @Getter(onMethod = @__({@Override}))
  private final List<String> libraryIds;

  private final Class<TDocument> documentType;

  protected ElasticsearchIterator(ElasticsearchTransport transport, SearchRequest searchRequest, Time scrollTimeout,
                                  Function<List<Hit<TDocument>>, List<T>> converter, List<String> libraryIds, Class<TDocument> type) {
    this.searchRequest = searchRequest;
    this.timeout = scrollTimeout;
    this.client = new ElasticsearchClient(transport);
    this.asyncClient = new ElasticsearchAsyncClient(transport);
    this.converter = converter;
    this.scrollIdRef = new AtomicReference<>();
    this.storageName = STORAGE_TYPE;
    this.libraryIds = libraryIds;
    this.documentType = type;
  }

  @Override
  public void close() {
    String scrollId = scrollIdRef.get();
    if (scrollId == null) {
      return;
    }
    var request = new ClearScrollRequest.Builder().scrollId(scrollId);
    asyncClient.clearScroll(request.build()).whenComplete((response, exception) -> {
      if (exception != null) {
        boolean clearScrollIdNotFound = Arrays.stream(exception.getSuppressed())
            .map(Throwable::getMessage)
            .anyMatch("Failed to parse object: expecting field with name [error] but found [succeeded]"::equals);
        if (clearScrollIdNotFound) {
          log.trace("Succeed clear scroll id : {}", scrollId);
        } else {
          log.error("Error clearing scroll context with scroll id : {}", scrollId, exception);
        }
      } else {
        log.debug("Clear scroll id : {}", scrollId);
      }
    });
  }

  @Override
  public List<T> next() {
    ResponseBody<TDocument> response;
    if (scrollIdRef.get() == null) {
      response = tryToSearch(searchRequest);
    } else {
      response = tryToScroll(new ScrollRequest.Builder()
          .scrollId(scrollIdRef.get())
          .scroll(timeout)
          .build());
    }
    scrollIdRef.set(response.scrollId());
    if (response.hits().hits().isEmpty()) {
      return List.of();
    }
    List<T> filteredResults = converter.apply(response.hits().hits());
    return filteredResults.isEmpty() ? next() : filteredResults;
  }

  private ResponseBody<TDocument> tryToSearch(SearchRequest searchRequest) {
    try {
      log.trace("Elastic client search method call with parameters {}", searchRequest);
      return client.search(searchRequest, documentType);
    } catch (ElasticsearchException e) {
      log.error("Exception in StorageElastic tryToSearch(). With input searchRequest {} and response {}.",
        searchRequest, e.response(), e);
      throw new ElasticsearchStorageException("Unable to search");
    } catch (Exception e) {
      log.error("Exception in StorageElastic tryToSearch(). With input searchRequest {}.", searchRequest, e);
      throw new ElasticsearchStorageException("Unable to search");
    }
  }

  private ResponseBody<TDocument> tryToScroll(ScrollRequest scrollRequest) {
    try {
      return client.scroll(scrollRequest, documentType);
    } catch (Exception e) {
      throw new ElasticsearchStorageException("Bad scroll request with id " + scrollRequest.scrollId(), e);
    }
  }

  /**
   * Converts to {@link Flattened.Molecule}.
   */
  public static class Molecules extends ElasticsearchIterator<MoleculeDocument, Flattened.Molecule> {

    public Molecules(ElasticsearchTransport transport, SearchRequest searchRequest, Time scrollTimeout,
                     Function<List<Hit<MoleculeDocument>>, List<Flattened.Molecule>> converter, List<String> libraryIds) {
      super(transport, searchRequest, scrollTimeout, converter, libraryIds, MoleculeDocument.class);
    }
  }

  /**
   * Converts to {@link Flattened.Reaction}.
   */
  public static class Reactions extends ElasticsearchIterator<Object, Flattened.Reaction> {

    public Reactions(ElasticsearchTransport transport, SearchRequest searchRequest, Time scrollTimeout,
                     Function<List<Hit<Object>>, List<Flattened.Reaction>> converter, List<String> libraryIds) {
      super(transport, searchRequest, scrollTimeout, converter, libraryIds, Object.class);
    }
  }

  /**
   * Converts to {@link Flattened.ReactionParticipant}.
   */
  public static class ReactionParticipants extends ElasticsearchIterator<Object, Flattened.ReactionParticipant> {

    public ReactionParticipants(ElasticsearchTransport transport, SearchRequest searchRequest, Time scrollTimeout,
                                Function<List<Hit<Object>>, List<Flattened.ReactionParticipant>> converter,
                                List<String> libraryIds) {
      super(transport, searchRequest, scrollTimeout, converter, libraryIds, Object.class);
    }
  }
}
