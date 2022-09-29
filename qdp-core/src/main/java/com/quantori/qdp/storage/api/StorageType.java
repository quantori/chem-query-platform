package com.quantori.qdp.storage.api;

/**
 * Supported in bingo storage types. Helps to distinguish implementation of storage.
 */
public enum StorageType {
  /**
   * The elasticsearch storage.
   *
   * @see <a href="https://www.elastic.com">Elasticsearch</a>
   */
  elasticsearch,
  /**
   * Solr storage.
   *
   * @see <a href="https://solr.apache.org/">Solr</a>
   */
  solr,
  /**
   * Database storage. Concrete RDMS is configurable.
   */
  db,
  /**
   * Opensearch storage.
   *
   * @see <a href="https://opensearch.org/">Opensearch</a>
   */
  opensearch,
  /**
   * Postgresql storage.
   *
   * @see <a href="https://www.postgresql.org/">Postgresql</a>
   */
  postgresql,
  /**
   * In memory storage.
   */
  memory
}
