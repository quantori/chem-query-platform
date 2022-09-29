package com.quantori.qdp.storage.solr;

import static com.quantori.qdp.storage.solr.SolrStorageReactions.REACTIONS_STORE_PREFIX;
import static com.quantori.qdp.storage.solr.SolrStorageReactions.REACTION_PARTICIPANTS_STORE_PREFIX;
import static java.util.Map.entry;
import static org.apache.solr.client.solrj.request.CollectionAdminRequest.createCollection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.quantori.qdp.storage.api.Library;
import com.quantori.qdp.storage.api.LibraryAlreadyExistsException;
import com.quantori.qdp.storage.api.LibraryType;
import com.quantori.qdp.storage.api.StorageLibrary;
import com.quantori.qdp.storage.api.StorageType;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.SolrInputDocument;

/**
 * Library management service for solr storage.
 */
@Slf4j
@RequiredArgsConstructor
public class SolrStorageLibrary implements StorageLibrary {
  private static final int COMMIT_WITHIN_MS = 1000;
  private static final ObjectMapper mapper = new ObjectMapper();

  static {
    mapper.registerModule(new JavaTimeModule());
  }

  private final String librariesCollection;
  private final boolean rollbackEnabled;
  private final HttpSolrClient solrClient;
  private final ConcurrentUpdateSolrClient solrUpdateClient;

  @Override
  public Optional<Library> getLibraryById(String libraryId) {
    try {
      return Optional.ofNullable(solrClient.getById(librariesCollection, libraryId))
          .map(Mapper::toLibrary);
    } catch (Exception e) {
      log.error("Unable to find library by id {} in {}", libraryId, librariesCollection, e);
      return Optional.empty();
    }
  }

  @Override
  public List<Library> getLibraryByName(String libraryName) {
    var solrQuery = new SolrQuery("*:*").addFilterQuery(String.format("%s:%s", "name", libraryName));
    try {
      var response = solrClient.query(librariesCollection, solrQuery);
      if (response.getStatus() != 0) {
        log.warn("Solr query returned with status 0, response = {}", response);
        return List.of();
      }
      return response.getResults().stream().map(Mapper::toLibrary).toList();
    } catch (SolrServerException | IOException e) {
      log.warn("Failed to search library {} within solr collection {}", libraryName, librariesCollection, e);
      return List.of();
    }
  }

  @Override
  public List<Library> getLibraryByType(LibraryType libraryType) {
    var query = new SolrQuery(String.format("type:%s", libraryType)).setRows(Integer.MAX_VALUE);
    try {
      var response = solrClient.query(librariesCollection, query);
      var results = response.getResults();
      return results.stream().map(Mapper::toLibrary).toList();
    } catch (SolrServerException | IOException e) {
      log.warn("SolrServerException|IOException in StorageSolr  getAllLibraries(). With input, libraryType:{}",
          libraryType, e);
    }
    return Collections.emptyList();
  }

  @Override
  public Library createLibrary(String libraryName, LibraryType libraryType, SolrServiceData serviceData)
      throws LibraryAlreadyExistsException {
    List<Library> libraries = getLibraryByName(libraryName);
    if (!libraries.isEmpty()) {
      throw new LibraryAlreadyExistsException(StorageType.solr, libraryName);
    }
    int shards = serviceData.getShards();
    int replicas = serviceData.getReplicas();
    var libraryId = UUID.randomUUID().toString();

    var wasCreated = switch (libraryType) {
      case molecules -> createMoleculesCollection(libraryId, shards, replicas);
      case reactions -> createReactionsLibrary(libraryId, shards, replicas);
      default -> false;
    };
    if (wasCreated) {
      var document = new SolrLibraryDocument(libraryId, libraryName, libraryType.name(), 0, ZonedDateTime.now());
      try {
        var updateResponse = solrUpdateClient.addBean(librariesCollection, document, COMMIT_WITHIN_MS);
        int status = updateResponse.getStatus();
        if (status != 0) {
          throw new SolrStorageException(String.format("Couldn't add library document: %s in libraries collection",
              document), String.valueOf(status));
        }
        solrUpdateClient.commit(librariesCollection);
        return new Library(document.getId(), document.getName(), libraryType, serviceData.toMap());
      } catch (Exception e) {
        log.warn("Failed to create library:{}", libraryName, e);
        final boolean b = rollbackCreatedCollections(libraryId, libraryType);
        if (!b) {
          log.warn("Couldn't rollback created collections properly.");
        }
        throw new SolrStorageException("Failed to register library " + libraryName, e);
      }
    }
    throw new SolrStorageException("Failed to create collection for library " + libraryName);
  }

  @Override
  public boolean deleteLibrary(Library library) {
    deleteCollectionAccordingToType(library);
    deleteLibraryDocumentById(library.getId());
    return true;
  }

  @Override
  public void updateLibrarySize(String libraryId, long count) {
    getLibraryById(libraryId).ifPresent(library -> {
      //solr doesn't support yet single field update
      SolrInputDocument document = new SolrInputDocument();
      document.setField("id", libraryId);
      document.setField("name", library.getName());
      document.setField("type", library.getType().toString());
      document.setField("updated_timestamp", library.getUpdatedStamp());
      document.setField("created_timestamp", library.getCreatedStamp());
      document.setField("structures_count", count);
      document.setField("properties", library.getProperties());
      try {
        final UpdateResponse res = solrUpdateClient.add(librariesCollection, document);
        solrUpdateClient.commit(librariesCollection);
        var status = res.getStatus();
        log.debug("Solr response on request to update document with {}", status);
        if (status != 0) {
          throw new SolrStorageException(String.format("Couldn't update library %s",
              library.getName()), String.valueOf(status));
        }
      } catch (SolrServerException | IOException | SolrStorageException e) {
        log.error("Failed to update library id:{} with size: {}", libraryId, count);
      }
    });
  }

  private boolean rollbackCreatedCollections(String collection, LibraryType libraryType) {
    if (libraryType == LibraryType.molecules) {
      return deleteCollectionWithName(collection);
    } else {
      final boolean b = deleteCollectionWithName(REACTIONS_STORE_PREFIX + collection);
      final boolean b1 = deleteCollectionWithName(REACTION_PARTICIPANTS_STORE_PREFIX + collection);
      return b && b1;
    }
  }

  private boolean createMoleculesCollection(String collectionName, int shards, int replicas) {
    try {
      //will create default nrt replicas
      var process = createCollection(collectionName, shards, replicas)
          .process(solrUpdateClient);
      if (process.getStatus() == 0) {
        initFields(collectionName, FieldFactory.moleculeFields);
        return true;
      }
    } catch (IOException | SolrServerException | InitFailedException e) {
      log.warn("Error during collection create serviceData:{}", collectionName, e);
      try {
        var process = CollectionAdminRequest.deleteCollection(collectionName)
            .process(solrUpdateClient);
        int status = process.getStatus();
        if (status != 0) {
          throw new SolrStorageException(String.format("Couldn't rollback molecules library: %s creation",
              collectionName), String.valueOf(status));
        }
        log.trace("Successfully rolled back molecules library : {}", collectionName);
      } catch (Exception ex) {
        log.warn("Failed to rollback create molecule library {}.", collectionName, ex);
      }
      return false;
    }
    return false;
  }

  private boolean createReactionsLibrary(String collectionName, int shards, int replicas) {
    var reactionsCollectionName = REACTIONS_STORE_PREFIX + collectionName;
    var participantsCollectionName = REACTION_PARTICIPANTS_STORE_PREFIX + collectionName;
    try {
      var processReactions =
          createCollection(reactionsCollectionName, shards, replicas).process(solrUpdateClient);
      var processParticipants =
          createCollection(participantsCollectionName, shards, replicas).process(solrUpdateClient);

      initFields(participantsCollectionName, FieldFactory.reactionParticipantsFields);
      initFields(reactionsCollectionName, FieldFactory.reactionFields);

      CollectionAdminRequest.reloadCollection(reactionsCollectionName);
      CollectionAdminRequest.reloadCollection(participantsCollectionName);

      if (processReactions.getStatus() != 0 || processParticipants.getStatus() != 0) {
        throw new SolrStorageException(String.format(
            "Couldn't create reaction library: %s, SOLR server status error: %d-reaction, %d-participants",
            collectionName, processReactions.getStatus(), processParticipants.getStatus()));
      }
      log.info("Successfully created reactions library: {}", collectionName);

      return true;
    } catch (IOException | SolrServerException | InitFailedException e) {
      log.warn("Exception in StorageSolr  createReactionsLibrary(). With input, library:{}", collectionName, e);
      try {
        var wasDeleted1 = CollectionAdminRequest.deleteCollection(reactionsCollectionName).process(solrUpdateClient);
        if (wasDeleted1.getStatus() != 0) {
          throw new SolrStorageException(String.format("Couldn't delete library: %s",
              reactionsCollectionName), String.valueOf(wasDeleted1.getStatus()));
        }
        var wasDeleted2 = CollectionAdminRequest.deleteCollection(participantsCollectionName).process(solrUpdateClient);
        if (wasDeleted2.getStatus() != 0) {
          throw new SolrStorageException(String.format("Couldn't delete library: %s",
              reactionsCollectionName), String.valueOf(wasDeleted1.getStatus()));
        }
        log.debug("Successfully rolled back reactions library creation for library: {}", collectionName);
      } catch (Exception solrServerException) {
        log.warn("Failed to rollback create reactions library {}", collectionName, e);
      }
      return false;
    }
  }

  protected void initFields(String collectionName, List<FieldDefinition> schemaFields)
      throws InitFailedException {
    try {
      var fieldRequest = new SchemaRequest.Fields();
      List<String> fields = fieldRequest.process(solrUpdateClient, collectionName).getFields().stream()
          .map(field -> (String) field.get("name")).toList();

      List<SchemaRequest.Update> updates = schemaFields.stream().map(fieldDefinition -> {
        if (!fields.contains(fieldDefinition.name())) {
          return new SchemaRequest.AddField(toFieldDefinitionMap(fieldDefinition));
        } else {
          return (SchemaRequest.Update) new SchemaRequest.ReplaceField(toFieldDefinitionMap(fieldDefinition));
        }
      }).toList();

      SchemaResponse.UpdateResponse result = new SchemaRequest.MultiUpdate(updates)
          .process(solrUpdateClient, collectionName);
      log.info("Collection {} schema update response status {}", collectionName, result.getStatus());
    } catch (Exception e) {
      throw new InitFailedException(collectionName, e);
    }
  }

  private boolean deleteCollectionWithName(String name) {
    var delete = CollectionAdminRequest.deleteCollection(name);
    try {
      var process = delete.process(solrUpdateClient);
      if (process.getStatus() != 0) {
        throw new SolrStorageException(String.format("Couldn't delete SOLR collection with name: %s ", name));
      }
      return true;
    } catch (Exception e) {
      log.warn("Failed to delete collection {}", name, e);
      return false;
    }
  }

  private void deleteCollectionAccordingToType(Library library) {
    if (LibraryType.molecules.equals(library.getType())) {
      if (!deleteCollectionWithName(library.getId())) {
        throw new SolrStorageException("Failed to delete molecule library " + library.getName());
      }
    } else if (LibraryType.reactions.equals(library.getType())) {
      var collectionName = REACTIONS_STORE_PREFIX + library.getId();
      var deleteReactionResult = deleteCollectionWithName(collectionName);
      if (!deleteReactionResult) {
        log.warn("Failed to delete reaction collection {}", collectionName);
      }
      var deleteReactionParticipantsResult =
          deleteCollectionWithName(REACTION_PARTICIPANTS_STORE_PREFIX + library.getId());
      var deleteResult = deleteReactionResult && deleteReactionParticipantsResult;
      if (!deleteResult) {
        throw new SolrStorageException("Failed to delete reaction library" + library.getName());
      }
    }
  }

  private void deleteLibraryDocumentById(String collectionName) {
    try {
      var query = String.format("id:%s", collectionName);
      var updateResponse = solrUpdateClient.deleteByQuery(librariesCollection, query);
      if (updateResponse.getStatus() != 0) {
        throw new SolrStorageException(String.format("Couldn't delete document with id: %s, from libraries collection",
            collectionName), String.valueOf(updateResponse.getStatus()));
      }
      solrUpdateClient.commit(librariesCollection, true, true);
    } catch (Exception e) {
      log.warn("Unable delete library id: {}. Trying to roll back.", collectionName, e);
      if (rollbackEnabled) {
        try {
          solrUpdateClient.rollback(librariesCollection);
        } catch (SolrServerException | IOException solrServerException) {
          log.error("Error while rollback", solrServerException);
        }
      }
    }
  }

  private Map<String, Object> toFieldDefinitionMap(FieldDefinition fieldDefinition) {
    return Map.ofEntries(entry("name", fieldDefinition.name()), entry("type", fieldDefinition.type()),
        entry("stored", fieldDefinition.stored()), entry("indexed", fieldDefinition.indexed()),
        entry("multiValued", fieldDefinition.multiValued()));
  }
}
