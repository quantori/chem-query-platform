package com.quantori.qdp.storage.solr;

import java.time.Duration;
import javax.validation.constraints.NotNull;
import lombok.Data;

@Data
public class SolrProperties {

  /**
   * URL address of solr instance.
   */
  @NotNull
  private String url = "http://localhost:8983/solr/";
  /**
   * Collection to hold libraries definitions.
   */
  @NotNull
  private String librariesCollectionName = "libraries";
  /**
   * Enable rollback on library create operation. If it is true, then error during operation library create will trigger
   * rollback operation and will try to delete previously created collection.
   */
  private boolean rollbackEnabled = true;

  /**
   * Timeout within which, document added to solr "collection" must be committed(sent to solr server).
   * Document internally added to queue of solr client and this parameter defines scheduler empty queue.
   */
  private int commitTimeout = -1;

  /**
   * Thread number to process queue of document in solr client.
   *
   * <p>Threads are created when documents are added to the client's internal queue and exit when no updates remain
   * in the queue. This value should be carefully paired with the maximum queue capacity.
   */
  private int threadCount = -1;
  /**
   * Queue size of cached document in solr client.
   *
   * <p>The maximum number of requests buffered by the SolrClient's internal queue before being processed by
   * background threads. This value should be carefully paired with the number of queue-consumer threads.
   * A queue with a maximum size set too high may require more memory. A queue with a maximum size set too low may
   * suffer decreased throughput
   */
  private int queueSize = -1;

  @NotNull
  private ConnectionOptions connection = new ConnectionOptions();

  /**
   * Http client connection configuration options: connection timeout and socket timeout.
   */
  @Data
  public static class ConnectionOptions {
    private static final Duration MINUTE = Duration.ofSeconds(60);

    /**
     * Http client connection timeout.
     */
    private Duration connectionTimeout = MINUTE;
    /**
     * Http client socket timeout.
     */
    private Duration socketTimeout = MINUTE;
  }
}
