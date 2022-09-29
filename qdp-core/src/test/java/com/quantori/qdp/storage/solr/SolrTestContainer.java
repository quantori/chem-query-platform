package com.quantori.qdp.storage.solr;

import lombok.Getter;
import org.testcontainers.containers.SolrContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

final class SolrTestContainer {
  @Container
  private static final SolrContainer solrContainer = new SolrContainer(DockerImageName.parse("solr:8.11.1"));
  @Getter
  private static String containerUrl;

  public static String startContainer() {
    if (solrContainer.isRunning()) {
      solrContainer.stop();
    }
    solrContainer.withZookeeper(true).start();
    containerUrl = String.format("http://%s:%s/solr", solrContainer.getHost(), solrContainer.getSolrPort());
    return containerUrl;
  }

}
