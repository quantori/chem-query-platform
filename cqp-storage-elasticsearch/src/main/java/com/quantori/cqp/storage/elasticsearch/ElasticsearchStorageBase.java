package com.quantori.cqp.storage.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Operator;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.transport.ElasticsearchTransport;
import com.quantori.cqp.api.util.FingerPrintUtilities;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.List;


@Slf4j
abstract class ElasticsearchStorageBase {

  static final int BATCH_PAGE_SIZE = 100;
  static final String CONFLICT_CODE = "409";

  final ElasticsearchTransport transport;
  final ElasticsearchClient client;
  final ElasticsearchAsyncClient asyncClient;

  final ElasticsearchProperties properties;
  final ElasticsearchStorageLibrary storageLibrary;

  public ElasticsearchStorageBase(ElasticsearchTransport transport, ElasticsearchProperties properties,
                                  ElasticsearchStorageLibrary storageLibrary) {
    this.transport = transport;
    this.client = new ElasticsearchClient(transport);
    this.asyncClient = new ElasticsearchAsyncClient(transport);
    this.properties = properties;
    this.storageLibrary = storageLibrary;
  }

  long countElements(String indexName) {
    var request = new CountRequest.Builder().index(indexName).build();
    try {
      return client.count(request).count();
    } catch (IOException e) {
      log.warn("Failed to count elements in index {} of storage {}", indexName,
        ElasticsearchStorageConfiguration.STORAGE_TYPE, e);
      throw new ElasticsearchStorageException("Failed to count elements", e);
    }
  }

  void updateLibraryDocument(String libraryId) {
    try {
      long count = countElements(libraryId);
      storageLibrary.updateLibrarySize(libraryId, count);
    } catch (ElasticsearchStorageException exception) {
      // multiple simultaneous requests to update library, try again
      // technically uploading by one molecule might create a lot of conflicts and throttling / accumulating
      // should be added, keep it simple for now
      if (CONFLICT_CODE.equals(exception.getErrorCode())) {
        updateLibraryDocument(libraryId);
      }
    }
  }

  BoolQuery.Builder searchStructureBuilder(byte[] queryFingerprint) {
    var sub = QueryBuilders.bool();
    var subHash = FingerPrintUtilities.substructureHash(queryFingerprint);

    if (!StringUtils.isEmpty(subHash)) {
      sub = sub.filter(QueryBuilders.match(mq ->
        mq.field(ElasticIndexMappingsFactory.SUB).query(subHash).operator(Operator.And).fuzzyTranspositions(false))
      );
    }

    return sub;
  }

  SearchRequest.Builder getSearchRequestBuilder(Query query) {
    return getSearchRequestBuilder(query, null, 0f);
  }

  SearchRequest.Builder getSearchRequestBuilder(Query query, List<SortOptions> sortOptions) {
    return getSearchRequestBuilder(query, sortOptions, 0f);
  }

  SearchRequest.Builder getSearchRequestBuilder(Query query, List<SortOptions> sortOptions, Float minSim) {
    SearchRequest.Builder builder = new SearchRequest.Builder()
      .minScore((double) (minSim == null ? 0 : minSim))
      .query(query)
      .size(BATCH_PAGE_SIZE)
      .timeout(timeValue(properties.getSearchTimeout()).time());
    if (sortOptions != null) {
      builder.sort(sortOptions);
    }
    return builder;
  }

  Time timeValue(Duration duration) {
    return Time.of(t -> t.time(duration.getSeconds() + "s"));
  }

  boolean deleteItemsAndUpdateLibrarySize(String libraryId, List<String> itemIds) {
    if (deleteItems(libraryId, itemIds) <= 0) {
      return false;
    }
    try {
      updateLibraryDocument(libraryId);
    } catch (Exception e) {
      // if item was removed, but we couldn't update count we still consider it as a success
      log.warn("An exception has been swallowed", e);
    }
    return true;
  }

  long deleteItems(String libraryId, List<String> itemIds) {
    var query = new Query.Builder()
      .ids(QueryBuilders.ids().values(itemIds).build())
      .build();
    return deleteByQuery(libraryId, query);
  }

  long deleteByQuery(String indexName, Query query) {
    var deleteRequest = new DeleteByQueryRequest.Builder()
      .index(indexName)
      .refresh(true)
      .query(query)
      .build();
    try {
      var response = client.deleteByQuery(deleteRequest);
      log.debug("Total deleted documents from index {} is {}", indexName, response.deleted());
      return response.deleted() == null ? 0 : response.deleted();
    } catch (IOException | ElasticsearchException exception) {
      log.error("Failed to execute request {}", deleteRequest, exception);
      return 0;
    }
  }
}
