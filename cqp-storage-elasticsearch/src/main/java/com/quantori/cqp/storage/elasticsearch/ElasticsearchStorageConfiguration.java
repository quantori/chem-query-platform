package com.quantori.cqp.storage.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quantori.cqp.api.MoleculesFingerprintCalculator;
import com.quantori.cqp.api.MoleculesMatcher;
import com.quantori.cqp.api.ReactionsFingerprintCalculator;
import com.quantori.cqp.api.ReactionsMatcher;
import com.quantori.cqp.api.StorageConfiguration;
import com.quantori.cqp.api.StorageLibrary;
import com.quantori.cqp.api.StorageMolecules;
import com.quantori.cqp.api.StorageReactions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.ByteArrayInputStream;

@Slf4j
public class ElasticsearchStorageConfiguration implements StorageConfiguration {

  public static final String STORAGE_TYPE = "cqpelasticsearch";
  public static final int STATUS_NOT_FOUND = 404;

  private final ElasticsearchStorageLibrary elasticsearchStorageLibrary;
  private final ElasticsearchStorageMolecules elasticsearchStorageMolecules;
  private final ElasticsearchStorageReactions elasticsearchStorageReactions;

  public ElasticsearchStorageConfiguration(
      ElasticsearchProperties properties, MoleculesMatcher moleculesMatcher,
      ReactionsMatcher reactionsMatcher, MoleculesFingerprintCalculator moleculesFingerprintCalculator,
      ReactionsFingerprintCalculator reactionsFingerprintCalculator
  ) {
    var transport = elasticsearchTransport(properties);
    var moleculesAllocator = moleculesResourceAllocator(transport);
    var reactionsAllocator = reactionsResourceAllocator(transport);
    var storagePropertiesMapping = storagePropertiesMapping(transport, moleculesAllocator, reactionsAllocator);

    elasticsearchStorageLibrary =
      new ElasticsearchStorageLibrary(properties.getLibraries(), transport, moleculesAllocator,
        reactionsAllocator, storagePropertiesMapping);
    elasticsearchStorageMolecules = new ElasticsearchStorageMolecules(transport, properties,
      elasticsearchStorageLibrary, storagePropertiesMapping, moleculesMatcher, moleculesFingerprintCalculator);
    elasticsearchStorageReactions = new ElasticsearchStorageReactions(transport, properties,
      elasticsearchStorageLibrary, reactionsMatcher, reactionsFingerprintCalculator);

    storagePostInit(properties, transport);
  }

  @Override
  public StorageLibrary getStorageLibrary() {
    return elasticsearchStorageLibrary;
  }

  @Override
  public StorageMolecules getStorageMolecules() {
    return elasticsearchStorageMolecules;
  }

  @Override
  public StorageReactions getStorageReactions() {
    return elasticsearchStorageReactions;
  }

  @Override
  public String storageType() {
    return STORAGE_TYPE;
  }

  @Override
  public ElasticsearchServiceData defaultServiceData() {
    return new ElasticsearchServiceData();
  }

  private ElasticsearchMoleculesResourceAllocator moleculesResourceAllocator(ElasticsearchTransport transport) {
    return new ElasticsearchMoleculesResourceAllocator(transport);
  }

  private ElasticsearchReactionsResourceAllocator reactionsResourceAllocator(ElasticsearchTransport transport) {
    return new ElasticsearchReactionsResourceAllocator(transport);
  }

  private ElasticsearchStoragePropertiesMapping storagePropertiesMapping(ElasticsearchTransport transport,
                                                                         ElasticsearchMoleculesResourceAllocator moleculesAllocator,
                                                                         ElasticsearchReactionsResourceAllocator reactionsAllocator) {
    return new ElasticsearchStoragePropertiesMapping(transport, moleculesAllocator, reactionsAllocator);
  }

  private ElasticsearchTransport elasticsearchTransport(ElasticsearchProperties properties) {
    var connectionOptions = properties.getConnection();
    var hosts = properties.getHosts();
    var authEnabled = StringUtils.isNotBlank(properties.getUsername());
    log.debug("Trying to establish connection with elastic cluster hosts {}", hosts);
    var httpHosts = hosts.stream().map(HttpHost::create).toArray(HttpHost[]::new);
    RestClientBuilder restClientBuilder = RestClient.builder(httpHosts)
      .setRequestConfigCallback(configBuilder -> {
//        configBuilder.setConnectTimeout((int) connectionOptions.getConnectionTimeout().toMillis());
//        configBuilder.setSocketTimeout((int) connectionOptions.getSocketTimeout().toMillis());
        configBuilder.setAuthenticationEnabled(authEnabled);
        return configBuilder;
      });
    if (authEnabled) {
      log.debug("Elasticsearch rest client authentication is enabled. Configuring authentication.");
      var credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(AuthScope.ANY,
        new UsernamePasswordCredentials(properties.getUsername(), properties.getPassword()));
      restClientBuilder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
        .setDefaultCredentialsProvider(credentialsProvider));
    }
    return new RestClientTransport(restClientBuilder.build(), new JacksonJsonpMapper(new ObjectMapper()));
  }

  private void storagePostInit(ElasticsearchProperties props, ElasticsearchTransport transport) {
    var client = new ElasticsearchAsyncClient(transport);
    ElasticScripts.update(client);
    tryCreateLibrariesIndex(props, client);
    log.info("Storage [elasticsearch] post action init is completed.");
  }

  private void tryCreateLibrariesIndex(ElasticsearchProperties props, ElasticsearchAsyncClient client) {
    var libraryIndexName = props.getLibraries();

    try {
      boolean indexExists = client.indices().exists(ExistsRequest.of(e -> e.index(libraryIndexName)))
        .get()
        .value();
      if (!indexExists) {
        createLibraryIndex(client, libraryIndexName);
      }
    } catch (Exception exception) {
      throw new ElasticsearchStorageException("Cannot check / create libraries index", exception);
    }
  }

  private void createLibraryIndex(ElasticsearchAsyncClient client, String libraryIndexName) {
    var request = CreateIndexRequest.of(b -> b
      .index(libraryIndexName).withJson(new ByteArrayInputStream(
        ElasticIndexMappingsFactory.LIBRARIES_MAPPING.getBytes()))
    );

    client.indices().create(request).whenComplete((response, exception) -> {
      if (exception != null) {
        log.error("Failed creating an index {}", libraryIndexName, exception);
      } else {
        log.info("Index created with result {}", response);
      }
    }).join();
  }
}
