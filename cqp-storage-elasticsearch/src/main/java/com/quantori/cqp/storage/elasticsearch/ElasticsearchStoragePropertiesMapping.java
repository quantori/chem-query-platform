package com.quantori.cqp.storage.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.indices.GetMappingResponse;
import co.elastic.clients.transport.ElasticsearchTransport;
import com.quantori.cqp.api.model.Library;
import com.quantori.cqp.api.model.Property;
import com.quantori.cqp.storage.elasticsearch.model.LibraryDocument;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
class ElasticsearchStoragePropertiesMapping {

  private final ElasticsearchClient client;
  private final ElasticsearchResourceAllocator moleculesAllocator;
  private final ElasticsearchResourceAllocator reactionsAllocator;

  public ElasticsearchStoragePropertiesMapping(ElasticsearchTransport transport,
                                               ElasticsearchResourceAllocator moleculesAllocator,
                                               ElasticsearchReactionsResourceAllocator reactionsAllocator) {
    this.client = new ElasticsearchClient(transport);
    this.moleculesAllocator = moleculesAllocator;
    this.reactionsAllocator = reactionsAllocator;
  }

  /**
   * Get properties mapping for a library
   *
   * @param libraryId       the identifier of a library
   * @param libraryDocument the library document containing properties mapping description
   * @return a map of property id and property object
   */
  public Map<String, Property> getPropertiesMapping(String libraryId, LibraryDocument libraryDocument) {
    return getPropertiesMapping(Map.of(libraryId, libraryDocument))
      .getOrDefault(libraryId, Map.of());
  }

  /**
   * Add new properties mapping to a library
   *
   * @param libraryId         the identifier of a library
   * @param libraryDocument   the library document containing existing properties mapping description
   * @param propertiesMapping new properties mapping to add
   * @return a status of an operation
   */
  public OperationStatus addPropertiesMapping(String libraryId, LibraryDocument libraryDocument,
                                              Map<String, Property> propertiesMapping) {
    if (propertiesMapping == null || propertiesMapping.isEmpty()) {
      return OperationStatus.failure();
    }

    Map<String, Property> existingPropertiesMapping = getPropertiesMapping(libraryId, libraryDocument);

    List<String> matchedKeys =
      propertiesMapping.keySet().stream()
        .filter(existingPropertiesMapping::containsKey)
        .toList();
    if (!matchedKeys.isEmpty()) {
      throw new ElasticsearchStorageException(
        String.format("A library %s already contains the following properties %s", libraryId,
          String.join(", ", matchedKeys)));
    }
    return createAndUpdatePropertiesMapping(libraryId, libraryDocument, propertiesMapping);
  }

  /**
   * Update existing properties mapping to a library
   *
   * @param libraryId         the identifier of a library
   * @param libraryDocument   the library document containing existing properties mapping description
   * @param propertiesMapping properties mapping to update
   * @return a status of an operation
   */
  public OperationStatus updatePropertiesMapping(String libraryId, LibraryDocument libraryDocument,
                                                 Map<String, Property> propertiesMapping) {
    if (propertiesMapping == null || propertiesMapping.isEmpty()) {
      return OperationStatus.failure();
    }

    Map<String, Property> existingPropertiesMapping = getPropertiesMapping(libraryId, libraryDocument);

    List<String> nonMatchedKeys =
      propertiesMapping.keySet().stream()
        .filter(key -> !existingPropertiesMapping.containsKey(key))
        .toList();
    if (!nonMatchedKeys.isEmpty()) {
      throw new ElasticsearchStorageException(
        String.format("A library %s does not contain the following properties %s", libraryId,
          String.join(", ", nonMatchedKeys)));
    }

    // ignore property type since it cannot be changed
    Map<String, Property> preparedPropertiesMapping = propertiesMapping.entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
        Property property = entry.getValue();
        return new Property(
          property.getName(),
          existingPropertiesMapping.get(entry.getKey()).getType(),
          property.getPosition(),
          property.isHidden(),
          property.isDeleted()
        );
      }, (a, b) -> a, LinkedHashMap::new));
    return createAndUpdatePropertiesMapping(libraryId, libraryDocument, preparedPropertiesMapping);
  }

  /**
   * Get properties mapping for a map of libraries
   *
   * @param libraryDocuments <code>Map<library_id, library_document></code>
   * @return <code>Map<library_id::String, Map <property_id::String, property::Property>></code>
   */
  public Map<String, Map<String, Property>> getPropertiesMapping(Map<String, LibraryDocument> libraryDocuments) {
    // use existing properties mapping from elastic index and mix names and flags to it from library document
    // this way we can return auto generated properties as well as have mix of autogenerated and
    // user specified properties in a library
    Map<String, Map<String, co.elastic.clients.elasticsearch._types.mapping.Property>> indexedProperties =
      getIndexedProperties(libraryDocuments.keySet().toArray(new String[0]));

    return indexedProperties.entrySet().stream()
      .collect(Collectors.toMap(
        // library_id
        Map.Entry::getKey,
        // map<property_id, property>
        entry -> convertElasticPropertiesToApplicationProperties(libraryDocuments.get(entry.getKey()),
          entry.getValue()),
        (a, b) -> a,
        LinkedHashMap::new
      ));
  }

  /**
   * Get elasticsearch indexed properties for libraries
   *
   * @param libraryIds an array of library identifiers
   * @return <code>Map<library_id::String, Map<property_id::String, elastic_property_object>></code>
   */
  private Map<String, Map<String, co.elastic.clients.elasticsearch._types.mapping.Property>> getIndexedProperties(
    String[] libraryIds) {
    GetMappingResponse mappingResponse = getIndexesMapping(libraryIds);

    return mappingResponse.result().entrySet().stream()
      .collect(Collectors.toMap(
        // libraryId
        Map.Entry::getKey,
        // map<property_id, elastic_property_object>
        entry -> {
          var indexMappingRecord = entry.getValue();
          return indexMappingRecord
            .mappings()
            .properties()
            .get(ElasticIndexMappingsFactory.MOL_PROPERTIES)
            .nested()
            .properties();
        },
        (a, b) -> a,
        LinkedHashMap::new));
  }

  private GetMappingResponse getIndexesMapping(String[] libraryIds) {
    var libraries = Arrays.stream(libraryIds).toList();
    try {
      return client.indices().getMapping(request -> request.index(libraries));
    } catch (ElasticsearchException exception) {
      log.error("An exception happened trying to describe indexes {}, response is {}", libraryIds, exception.response(),
        exception);
      throw new ElasticsearchStorageException(
        "Cannot determine property types on libraries " + String.join(",", libraries));
    } catch (Exception exception) {
      log.error("An exception happened trying to describe indexes {}", libraryIds, exception);
      throw new ElasticsearchStorageException(
        "Cannot determine property types on libraries " + String.join(",", libraries));
    }
  }

  private Map<String, Property> convertElasticPropertiesToApplicationProperties(
    LibraryDocument libraryDocument,
    Map<String, co.elastic.clients.elasticsearch._types.mapping.Property> indexedProperties) {
    return indexedProperties.entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, entry -> {
          Property property = libraryDocument.getPropertiesMapping().getOrDefault(entry.getKey(), null);
          Property.PropertyType type = ElasticIndexMappingsFactory.KINDS_MAP.get(entry.getValue()._kind());
          if (property == null) {
            return new Property(entry.getKey(), type);
          } else {
            return new Property(
              property.getName(),
              type,
              property.getPosition(),
              property.isHidden(),
              property.isDeleted()
            );
          }
        }, (a, b) -> a, LinkedHashMap::new)
      );
  }

  private OperationStatus createAndUpdatePropertiesMapping(String libraryId, LibraryDocument libraryDocument,
                                                           Map<String, Property> propertiesMapping) {
    Library library = Mapper.toLibrary(libraryId, libraryDocument);
    var updatable = switch (library.getType()) {
      case molecules -> moleculesAllocator;
      case reactions -> reactionsAllocator;
      default -> throw new ElasticsearchStorageException(
        "Not supported operation create library for library " + libraryDocument.getName());
    };
    try {
      boolean status = updatable.updateIndexPropertiesMapping(libraryId, propertiesMapping);
      if (!status) {
        return OperationStatus.failure();
      }
      Mapper.addPropertiesMappingToLibraryDocument(libraryDocument, propertiesMapping);
      return OperationStatus.success(libraryDocument);
    } catch (IOException e) {
      throw new ElasticsearchStorageException("Failed to add mapping to library " + libraryDocument.getName(), e);
    }
  }

  /**
   * Get a property name in an elastic index
   *
   * @param propertyName      property name
   * @param propertiesMapping <code>Map<library_id::String, Map<property_id::String, property::Property>></code>
   * @return optional of property field name
   */
  public Optional<String> getPropertyFieldName(String propertyName,
                                               Map<String, Map<String, Property>> propertiesMapping) {
    return getPropertyType(propertyName, propertiesMapping)
      .map(type -> ElasticIndexMappingsFactory.MOL_PROPERTIES_FIELD + propertyName);
  }

  /**
   * Get a property type
   *
   * @param propertyName      property name
   * @param propertiesMapping <code>Map<library_id::String, Map<property_id::String, property::Property>></code>
   * @return optional of property type
   */
  public Optional<Property.PropertyType> getPropertyType(String propertyName,
                                                         Map<String, Map<String, Property>> propertiesMapping) {
    // check that property has same type in all libraries
    List<Property.PropertyType> types =
      propertiesMapping.values().stream()
        .map(properties -> properties.get(propertyName))
        .map(property -> {
          if (property == null) {
            return null;
          } else {
            return property.getType();
          }
        })
        .distinct()
        .toList();

    if (types.size() != 1 || types.get(0) == null) {
      return Optional.empty();
    }
    return Optional.of(types.get(0));
  }

  record OperationStatus(boolean status, LibraryDocument updatedLibraryDocument) {
    public static OperationStatus success(LibraryDocument libraryDocument) {
      return new OperationStatus(true, libraryDocument);
    }

    public static OperationStatus failure() {
      return new OperationStatus(false, null);
    }
  }
}
