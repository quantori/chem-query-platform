package com.quantori.cqp.storage.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.json.JsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import com.quantori.cqp.api.model.Property;
import jakarta.json.stream.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class ElasticsearchMoleculesResourceAllocator implements ElasticsearchResourceAllocator {

  private final ElasticsearchClient client;
  private final ElasticsearchTransport transport;

  public ElasticsearchMoleculesResourceAllocator(ElasticsearchTransport transport) {
    this.transport = transport;
    this.client = new ElasticsearchClient(transport);
  }

  @Override
  public boolean createIndex(String libraryId, Map<String, Property> propertiesMapping, int shards, int replicas)
    throws IOException {
    var request = new CreateIndexRequest.Builder().index(libraryId);
    var mapping = String.format(ElasticIndexMappingsFactory.MOLECULES_MAPPING, shards, replicas,
      ElasticsearchFingerprintUtilities.FINGERPRINT_SIZE, getPropertiesMapping(propertiesMapping));
    request.withJson(new ByteArrayInputStream(mapping.getBytes()));
    var response = client.indices().create(request.build());
    return response.acknowledged();
  }

  @Override
  public boolean updateIndexPropertiesMapping(String libraryId, Map<String, Property> propertiesMapping)
    throws IOException {
    var indexMapping = String.format(ElasticIndexMappingsFactory.MOLECULES_UPDATE_INDEX_MAPPING,
      getPropertiesMapping(propertiesMapping));
    JsonpMapper mapper = transport.jsonpMapper();
    try (JsonParser parser = mapper.jsonProvider().createParser(new StringReader(indexMapping))) {
      var response =
        client.indices().putMapping(r -> r.index(libraryId).properties(ElasticIndexMappingsFactory.MOL_PROPERTIES,
          co.elastic.clients.elasticsearch._types.mapping.Property._DESERIALIZER.deserialize(parser, mapper)));
      return response.acknowledged();
    }
  }

  @Override
  public boolean removeIndex(String libraryId) throws IOException {
    var request = new DeleteIndexRequest.Builder().index(libraryId).build();
    var response = client.indices().delete(request);
    log.debug("Delete index response acknowledged {} of index {}", response.acknowledged(), libraryId);
    return true;
  }

  private String getPropertiesMapping(Map<String, Property> propertiesMapping) {
    if (propertiesMapping == null || propertiesMapping.isEmpty()) {
      return "";
    }

    return propertiesMapping.entrySet().stream()
      .filter(entry -> StringUtils.isNotBlank(entry.getKey()))
      .filter(entry -> Objects.nonNull(entry.getValue()))
      .map(entry -> {
        String propertyKey = entry.getKey();
        Property property = entry.getValue();
        Property.PropertyType type = Objects.requireNonNullElse(property.getType(), Property.PropertyType.STRING);
        String elasticType = ElasticIndexMappingsFactory.TYPES_MAP.get(type);
        String typeName = type.name();
        if (Property.PropertyType.DATE == type || Property.PropertyType.DATE_TIME == type) {
          return String.format(
            ElasticIndexMappingsFactory.DATE_PROPERTY_MAPPING,
            propertyKey,
            elasticType,
            ElasticIndexMappingsFactory.DATE_FORMAT_PATTERN,
            ElasticIndexMappingsFactory.PROPERTY_TYPE_META_KEY,
            typeName
          );
        }
        return String.format(
          ElasticIndexMappingsFactory.PROPERTY_MAPPING,
          propertyKey,
          elasticType,
          ElasticIndexMappingsFactory.PROPERTY_TYPE_META_KEY,
          typeName
        );
      })
      .collect(Collectors.joining(",\n", ",\"" + ElasticIndexMappingsFactory.PROPERTIES + "\": {\n", "}\n"));
  }
}
