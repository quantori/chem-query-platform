package com.quantori.cqp.storage.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.transport.ElasticsearchTransport;
import com.quantori.cqp.api.model.Property;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

@Slf4j
@RequiredArgsConstructor
public class ElasticsearchReactionsResourceAllocator implements ElasticsearchResourceAllocator {

  private final ElasticsearchClient client;

  public ElasticsearchReactionsResourceAllocator(ElasticsearchTransport transport) {
    this.client = new ElasticsearchClient(transport);
  }

  @Override
  public boolean createIndex(String libraryId, Map<String, Property> propertiesMapping, int shards, int replicas)
    throws IOException {
    var reactionsIndexName = ElasticsearchStorageReactions.getReactionIndexName(libraryId);
    boolean actualReactionsIndexCreated = createActualReactionsIndex(reactionsIndexName, shards, replicas);
    if (!actualReactionsIndexCreated) {
      return false;
    }

    var reactionParticipantsIndexName = ElasticsearchStorageReactions.getReactionParticipantIndexName(libraryId);
    try {
      if (createReactionParticipantsIndex(reactionParticipantsIndexName, shards, replicas)) {
        return true;
      }
      deleteIndexWithName(reactionsIndexName);
      return false;
    } catch (Exception e) {
      log.error("Failed to create index '{}' in elastic", reactionParticipantsIndexName, e);
      try {
        deleteIndexWithName(reactionsIndexName);
      } catch (Exception e2) {
        log.error("Failed to delete partially created index '{}' in elastic", reactionsIndexName, e2);
      }
      throw e;
    }
  }

  @Override
  public boolean updateIndexPropertiesMapping(String libraryId, Map<String, Property> propertiesMapping) {
    return false;
  }

  @Override
  public boolean removeIndex(String libraryId) {
    var reactionIndexName = ElasticsearchStorageReactions.getReactionIndexName(libraryId);
    deleteIndexWithName(reactionIndexName);
    var reactionParticipantsIndexName = ElasticsearchStorageReactions.getReactionParticipantIndexName(libraryId);
    deleteIndexWithName(reactionParticipantsIndexName);
    return true;
  }

  private boolean createActualReactionsIndex(String indexName, int shards, int replicas)
    throws IOException, ElasticsearchException {
    return createIndex(indexName, ElasticIndexMappingsFactory.ACTUAL_REACTION_MAPPING, shards, replicas);
  }

  private boolean createReactionParticipantsIndex(String indexName, int shards, int replicas)
    throws IOException, ElasticsearchException {
    return createIndex(indexName, ElasticIndexMappingsFactory.REACTION_PARTICIPANTS_MAPPING, shards, replicas);
  }

  private boolean createIndex(String indexName, String source, int shards, int replicas)
    throws IOException, ElasticsearchException {
    var request = new CreateIndexRequest.Builder().index(indexName)
      .withJson(new ByteArrayInputStream(String.format(source, shards, replicas).getBytes())).build();

    var response = client.indices().create(request);
    log.debug("Elastic response to create index : {}", indexName);
    return response.acknowledged();
  }

  private boolean deleteIndexWithName(String indexName) {
    try {
      var request = new DeleteIndexRequest.Builder().index(indexName).build();
      client.indices().delete(request);
      return true;
    } catch (ElasticsearchException exception) {
      if (exception.status() == ElasticsearchStorageConfiguration.STATUS_NOT_FOUND) {
        log.warn("Failed to remove index {} because index not found", indexName);
        return true;
      } else {
        log.error("Failed to remove index {}", indexName, exception);
        return false;
      }
    } catch (IOException exception) {
      log.error("Failed to remove index {}", indexName, exception);
      return false;
    }
  }
}
