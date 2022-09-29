package com.quantori.qdp.storage.solr;

import static java.util.Map.entry;

import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;

@Slf4j
public class SolrStorageConfiguration {
  private final SolrProperties props;
  private final String libraryCollectionName;
  private final String url;

  @Getter
  private final HttpSolrClient solrClient;
  @Getter
  private final ConcurrentUpdateSolrClient solrUpdateClient;

  public SolrStorageConfiguration(SolrProperties solrProperties) {
    this.props = solrProperties;
    this.url = solrProperties.getUrl();
    this.libraryCollectionName = solrProperties.getLibrariesCollectionName();
    solrClient = solrClient();
    solrUpdateClient = solrUpdateClient();
    initLibraryCollection(solrClient, solrUpdateClient);
  }

  private ConcurrentUpdateSolrClient solrUpdateClient() {
    var connectionOptions = props.getConnection();
    var builder =
        new ConcurrentUpdateSolrClient.Builder(url)
            .withConnectionTimeout((int) connectionOptions.getConnectionTimeout().toMillis())
            .withSocketTimeout((int) connectionOptions.getSocketTimeout().toMillis());
    if (props.getQueueSize() > 0) {
      builder.withQueueSize(props.getQueueSize());
    }
    if (props.getThreadCount() > 0) {
      builder.withThreadCount(props.getThreadCount());
    }
    var client = builder.build();
    log.info("Update solr client is ready with url: {}", url);
    return client;
  }


  private HttpSolrClient solrClient() {
    var client = new HttpSolrClient.Builder(url).build();
    log.info("Search solr client is ready with url {}", url);
    return client;
  }

  private void initLibraryCollection(SolrClient solrClient, ConcurrentUpdateSolrClient solrUpdateClient) {
    try {
      if (CollectionAdminRequest.listCollections(solrClient).contains(libraryCollectionName)) {
        return;
      }
      var process = CollectionAdminRequest
          .createCollection(libraryCollectionName, 1, 1).process(solrUpdateClient);
      if (process.getStatus() != 0) {
        log.error("Create libraries collection response has bad status {}", process.getStatus());
        return;
      }
      initFields(solrUpdateClient);
      log.trace("Successfully created libraries collection");
    } catch (InitFailedException e) {
      log.warn("Init error", e);
    } catch (Exception e) {
      log.warn("Unknown error during initialization of collection {}", libraryCollectionName, e);
    }
  }

  private void initFields(ConcurrentUpdateSolrClient solrUpdateClient) throws InitFailedException {
    try {
      var fieldRequest = new SchemaRequest.Fields();
      List<String> fields = fieldRequest.process(solrUpdateClient, libraryCollectionName).getFields().stream()
          .map(field -> (String) field.get("name"))
          .toList();

      List<SchemaRequest.Update> updates = FieldFactory.libraryFields.stream().map(fieldDefinition -> {
        if (!fields.contains(fieldDefinition.name())) {
          return new SchemaRequest.AddField(toFieldDefinitionMap(fieldDefinition));
        } else {
          return (SchemaRequest.Update) new SchemaRequest.ReplaceField(toFieldDefinitionMap(fieldDefinition));
        }
      }).toList();

      SchemaResponse.UpdateResponse result =
          new SchemaRequest.MultiUpdate(updates).process(solrUpdateClient, libraryCollectionName);
      log.info("Collection {} schema update response status {}", libraryCollectionName, result.getStatus());
    } catch (Exception e) {
      throw new InitFailedException(libraryCollectionName, e);
    }
  }

  private Map<String, Object> toFieldDefinitionMap(FieldDefinition fieldDefinition) {
    return Map.ofEntries(entry("name", fieldDefinition.name()), entry("type", fieldDefinition.type()),
        entry("stored", fieldDefinition.stored()), entry("indexed", fieldDefinition.indexed()),
        entry("multiValued", fieldDefinition.multiValued()));
  }

}
