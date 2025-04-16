package com.quantori.cqp.storage.elasticsearch;

import com.quantori.cqp.api.model.Property;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public interface ElasticsearchResourceAllocator {

  default Optional<String> allocateResources(Map<String, Property> propertiesMapping,
                                             Map<String, Object> serviceData) throws IOException {
    var libraryId = UUID.randomUUID().toString();
    int shards = (Integer) serviceData.get(ElasticsearchServiceData.Fields.shards);
    int replicas = (Integer) serviceData.get(ElasticsearchServiceData.Fields.replicas);
    if (createIndex(libraryId, propertiesMapping, shards, replicas)) {
      return Optional.of(libraryId);
    } else {
      return Optional.empty();
    }
  }

  boolean createIndex(String libraryId, Map<String, Property> propertiesMapping, int shards, int replicas)
    throws IOException;

  boolean updateIndexPropertiesMapping(String libraryId, Map<String, Property> propertiesMapping) throws IOException;

  boolean removeIndex(String libraryId) throws IOException;
}
