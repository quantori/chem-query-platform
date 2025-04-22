package com.quantori.cqp.storage.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.InlineScript;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.Script;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch.core.DeleteByQueryRequest;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.UpdateByQueryRequest;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.transport.ElasticsearchTransport;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.quantori.cqp.api.model.Library;
import com.quantori.cqp.api.model.LibraryType;
import com.quantori.cqp.api.model.Property;
import com.quantori.cqp.api.service.StorageLibrary;
import com.quantori.cqp.storage.elasticsearch.model.LibraryDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
class ElasticsearchStorageLibrary implements StorageLibrary {
  private final String libraryIndexName;
  private final int maxLibrariesCount;
  private final ElasticsearchClient client;

  private final ElasticsearchResourceAllocator moleculesAllocator;
  private final ElasticsearchResourceAllocator reactionsAllocator;

  private final ElasticsearchStoragePropertiesMapping storagePropertiesMapping;

  public ElasticsearchStorageLibrary(String libraryIndexName, ElasticsearchTransport transport,
                                     ElasticsearchMoleculesResourceAllocator moleculesResourceAllocator,
                                     ElasticsearchReactionsResourceAllocator reactionsResourceAllocator,
                                     ElasticsearchStoragePropertiesMapping storagePropertiesMapping) {
    this.libraryIndexName = libraryIndexName;
    this.maxLibrariesCount = 100;
    this.client = new ElasticsearchClient(transport);

    this.moleculesAllocator = moleculesResourceAllocator;
    this.reactionsAllocator = reactionsResourceAllocator;

    this.storagePropertiesMapping = storagePropertiesMapping;
  }

  @Override
  public Optional<Library> getLibraryById(String libraryId) {
    return tryToGetLibraryDocument(libraryId)
        .map(libraryDocument -> Mapper.toLibrary(libraryId, libraryDocument));
  }

  List<Library> getLibraryById(List<String> libraryIds) {
    return tryToGetLibraryDocuments(libraryIds.toArray(new String[0]))
        .entrySet()
        .stream()
        .map(entry -> Mapper.toLibrary(entry.getKey(), entry.getValue()))
        .toList();
  }

  @Override
  public List<Library> getLibraryByName(String libraryName) {
    var request = new SearchRequest.Builder().index(libraryIndexName).size(maxLibrariesCount).query(q -> q
        .match(t -> t.field(LibraryDocument.Fields.name).query(libraryName))).build();

    return tryToGetLibraryDocuments(request).entrySet().stream()
        .map(entry -> Mapper.toLibrary(entry.getKey(), entry.getValue()))
        .toList();
  }

  @Override
  public List<Library> getLibraryByType(LibraryType libraryType) {
    var request = new SearchRequest.Builder().index(libraryIndexName).size(maxLibrariesCount).query(q -> q
        .match(t -> t.field(LibraryDocument.Fields.type).query(libraryType.toString()))).build();

    return tryToGetLibraryDocuments(request).entrySet().stream()
        .map(entry -> Mapper.toLibrary(entry.getKey(), entry.getValue()))
        .toList();
  }

  @Override
  public Map<String, Property> getPropertiesMapping(String libraryId) {
    return getPropertiesMapping(new String[] {libraryId})
        .getOrDefault(libraryId, Map.of());
  }

  Map<String, Map<String, Property>> getPropertiesMapping(String[] libraryIds) {
    Map<String, LibraryDocument> libraryDocuments = tryToGetLibraryDocuments(libraryIds);
    if (libraryDocuments.isEmpty()) {
      return Map.of();
    }

    return storagePropertiesMapping.getPropertiesMapping(libraryDocuments);
  }

  @Override
  public Library createLibrary(String libraryName, LibraryType libraryType, Map<String, Property> propertiesMapping,
                               Map<String, Object> serviceData) {
    var creatable = switch (libraryType) {
      case molecules -> moleculesAllocator;
      case reactions -> reactionsAllocator;
      default -> throw new ElasticsearchStorageException(
          "Not supported operation create library for library " + libraryName);
    };

    String resourceId;
    try {
      resourceId = creatable.allocateResources(propertiesMapping, serviceData)
          .orElseThrow(() -> new ElasticsearchStorageException("Failed to allocate library resource " + libraryName));
    } catch (IOException e) {
      throw new ElasticsearchStorageException("Failed to allocate library resource " + libraryName, e);
    }

    try {
      return registerLibrary(resourceId, libraryName, libraryType, propertiesMapping);
    } catch (IOException e) {
      log.warn(
          "Some resource allocated for libraries left unbound due failed library register operation {}", libraryName);
      throw new ElasticsearchStorageException("Failed to register library " + libraryName, e);
    }
  }

  private Library registerLibrary(String resourceId, String libraryName, LibraryType libraryType,
                                  Map<String, Property> propertiesMapping) throws IOException {
    var libraryDocument = Mapper.toLibraryDocument(libraryName, libraryType, propertiesMapping);
    indexLibraryDocument(resourceId, libraryDocument);
    return Mapper.toLibrary(resourceId, libraryDocument);
  }

  void indexLibraryDocument(String libraryId, LibraryDocument libraryDocument) throws IOException {
    var request = new IndexRequest.Builder<>()
        .index(libraryIndexName)
        .id(libraryId)
        .withJson(new ByteArrayInputStream(JsonMapper.toJsonString(libraryDocument).getBytes()))
        .refresh(Refresh.WaitFor)
        .build();
    log.trace("Elasticsearch client index method call with parameters {}, {}", request, RequestOptions.DEFAULT);
    client.index(request);
  }

  void indexLibraryDocumentSilently(String libraryId, LibraryDocument libraryDocument) {
    try {
      indexLibraryDocument(libraryId, libraryDocument);
    } catch (ElasticsearchException exception) {
      log.warn("Cannot update library document because of reason {}", exception.response(), exception);
    } catch (IOException exception) {
      log.warn("Cannot update library document after updating index", exception);
    }
  }

  @Override
  public boolean addPropertiesMapping(String libraryId, Map<String, Property> propertiesMapping) {
    Optional<LibraryDocument> optionalLibraryDocument = tryToGetLibraryDocument(libraryId);
    if (optionalLibraryDocument.isEmpty()) {
      return false;
    }

    LibraryDocument libraryDocument = optionalLibraryDocument.get();
    ElasticsearchStoragePropertiesMapping.OperationStatus operationStatus =
        storagePropertiesMapping.addPropertiesMapping(libraryId, libraryDocument, propertiesMapping);
    if (operationStatus.status()) {
      indexLibraryDocumentSilently(libraryId, operationStatus.updatedLibraryDocument());
    }
    return operationStatus.status();
  }

  @Override
  public boolean updatePropertiesMapping(String libraryId, Map<String, Property> propertiesMapping) {
    Optional<LibraryDocument> optionalLibraryDocument = tryToGetLibraryDocument(libraryId);
    if (optionalLibraryDocument.isEmpty()) {
      return false;
    }

    LibraryDocument libraryDocument = optionalLibraryDocument.get();
    ElasticsearchStoragePropertiesMapping.OperationStatus operationStatus =
        storagePropertiesMapping.updatePropertiesMapping(libraryId, libraryDocument, propertiesMapping);
    if (operationStatus.status()) {
      indexLibraryDocumentSilently(libraryId, operationStatus.updatedLibraryDocument());
    }
    return operationStatus.status();
  }

  void updateLibrarySize(String libraryId, long count) {
    // update only if size changed
    Library existingLibrary = getLibraryById(libraryId)
        .orElseThrow(() -> new ElasticsearchStorageException("Could not find library with id " + libraryId));
    if (Objects.equals(count, existingLibrary.getStructuresCount())) {
      return;
    }

    updateLibrary(libraryId, Map.of("size", count));
  }

  @Override
  public Library updateLibrary(Library library) {
    // update only if name or size changed
    Library existingLibrary = getLibraryById(library.getId())
        .orElseThrow(() -> new ElasticsearchStorageException("Could not find library with id " + library.getId()));
    if (Objects.equals(library.getName(), existingLibrary.getName())
        && Objects.equals(library.getStructuresCount(), existingLibrary.getStructuresCount())) {
      return existingLibrary;
    }
    existingLibrary.setName(library.getName());
    existingLibrary.setStructuresCount(library.getStructuresCount());
    existingLibrary.setUpdatedStamp(Mapper.toInstant());

    updateLibrary(
        library.getId(),
        Map.of(
            "name", existingLibrary.getName(),
            "size", existingLibrary.getStructuresCount(),
            "updated_timestamp", Mapper.toTimestamp(existingLibrary.getUpdatedStamp())
        )
    );
    return existingLibrary;
  }

  private void updateLibrary(String libraryId, Map<String, Object> properties) {
    String source = properties.entrySet().stream()
        .map(entry -> "ctx._source." + entry.getKey() + "=" + wrapIfNotNull(entry.getValue()))
        .collect(Collectors.joining(";"));

    InlineScript script = new InlineScript.Builder()
        .lang("painless")
        .source(source)
        .build();

    var updateByQueryRequest = new UpdateByQueryRequest.Builder()
        .index(libraryIndexName)
        .query(QueryBuilders.ids(e -> e.values(libraryId)))
        .refresh(true)
        .script(new Script.Builder().inline(script).build())
        .build();
    try {
      var response = client.updateByQuery(updateByQueryRequest);
      log.debug("Update library {} response {}", libraryId, response);
      if (Objects.requireNonNullElse(response.updated(), 0L) <= 0) {
        throw new ElasticsearchStorageException("Could not update library with id: " + libraryId);
      }
    } catch (ElasticsearchException e) {
      // it looks like this exception is an internal client error
      log.error("Failed to execute request on library update {}, with request {}", libraryId, updateByQueryRequest, e);
      throw new ElasticsearchStorageException("Failed to update library " + libraryId);
    } catch (ResponseException e) {
      // this error is transmitted from Elasticsearch explaining internal Elasticsearch error
      log.error("Failed to update library {} in elasticsearch with request {}", libraryId, updateByQueryRequest, e);
      int code = e.getResponse().getStatusLine().getStatusCode();
      throw new ElasticsearchStorageException("Failed to update library " + libraryId, code);
    } catch (IOException exception) {
      log.error("Failed to execute request {}", updateByQueryRequest, exception);
      throw new ElasticsearchStorageException("Failed to update library for library " + libraryId);
    }
  }

  private String wrapIfNotNull(Object object) {
    return object == null ? null : "\"" + object + "\"";
  }

  @Override
  public Map<String, Object> getDefaultServiceData() {
    return new ElasticsearchServiceData().toMap();
  }

  @Override
  public boolean deleteLibrary(Library library) {
    if (library.getType() != LibraryType.reactions &&
        library.getType() != LibraryType.molecules) {
      throw new ElasticsearchStorageException("A library type must be molecule or reaction");
    }

    // delete library from libraries index
    boolean deleted = deleteLibraryById(library.getId());

    // delete library indexes for molecules / reactions
    if (deleted) {
      var removable = switch (library.getType()) {
        case molecules -> moleculesAllocator;
        case reactions -> reactionsAllocator;
        default -> throw new ElasticsearchStorageException(
            "Not supported operation remove library for library " + library.getId());
      };

      try {
        return removable.removeIndex(library.getId());
      } catch (ElasticsearchException exception) {
        if (exception.status() == ElasticsearchStorageConfiguration.STATUS_NOT_FOUND) {
          log.warn("Failed to remove index {} because index not found", library.getId());
        } else {
          log.error("Failed to remove index {}", library.getId(), exception);
        }
      } catch (IOException exception) {
        log.error("Failed to remove index {}", library.getId(), exception);
      }
    }

    return deleted;
  }

  private boolean deleteLibraryById(String libraryId) {
    var deleteRequest = new DeleteByQueryRequest.Builder()
        .index(libraryIndexName)
        .refresh(true)
        .query(QueryBuilders.ids()
            .values(libraryId)
            .build()
            ._toQuery()
        )
        .build();
    try {
      var response = client.deleteByQuery(deleteRequest);
      log.debug("Total deleted documents from index {} is {}", libraryIndexName, response.deleted());
      return response.deleted() != null && response.deleted() > 0;
    } catch (IOException | ElasticsearchException exception) {
      log.error("Failed to execute request {}", deleteRequest, exception);
      return false;
    }
  }

  private Optional<SearchResponse<LibraryDocument>> tryToSearch(SearchRequest searchRequest) {
    try {
      log.trace("Elastic client getLibrary method call {}, {}", searchRequest, RequestOptions.DEFAULT);
      var searchResponse = client.search(searchRequest, LibraryDocument.class);
      return Optional.of(searchResponse);
    } catch (IOException e) {
      log.error("Exception in StorageElastic tryToSearch(). With input searchRequest: {}.", searchRequest, e);
      return Optional.empty();
    }
  }

  Optional<LibraryDocument> tryToGetLibraryDocument(String libraryId) {
    return Optional.ofNullable(
        tryToGetLibraryDocuments(new String[] {libraryId})
            .get(libraryId)
    );
  }

  private Map<String, LibraryDocument> tryToGetLibraryDocuments(String[] libraryIds) {
    var request = new SearchRequest.Builder().index(libraryIndexName)
        .query(QueryBuilders.ids(e -> e.values(List.of(libraryIds))))
        .size(libraryIds.length).build();

    return tryToGetLibraryDocuments(request);
  }

  private Map<String, LibraryDocument> tryToGetLibraryDocuments(SearchRequest request) {
    return tryToSearch(request)
        .stream()
        .flatMap(response -> response.hits().hits().stream())
        .flatMap(hit -> tryToGetLibraryDocument(hit).stream()
            .map(libraryDocument -> Pair.of(hit.id(), libraryDocument)))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue, (a, b) -> a, LinkedHashMap::new));
  }

  private Optional<LibraryDocument> tryToGetLibraryDocument(Hit<LibraryDocument> hit) {
    String json = JsonMapper.objectToJSON(hit.source());
    try {
      var libraryDocument = JsonMapper.toLibraryDocument(json);
      log.trace("Molecule {} with score {}", json, hit.score());
      return Optional.of(libraryDocument);
    } catch (JsonProcessingException e) {
      log.warn("Failed to parse json {} to {}. Empty response returned", json, LibraryDocument.class, e);
      return Optional.empty();
    }
  }
}
