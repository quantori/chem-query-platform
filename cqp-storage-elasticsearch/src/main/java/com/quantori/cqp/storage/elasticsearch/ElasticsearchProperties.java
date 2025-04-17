package com.quantori.cqp.storage.elasticsearch;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.PositiveOrZero;
import lombok.Data;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Data
public class ElasticsearchProperties {
    private static final String LIBRARIES_INDEX_NAME = "libraries";

    /**
     * List of elasticsearch nodes addresses. Each address must be a valid <em>URI</em> in a form of
     * {@code http://23.234.12.98:9200}. If schema name is missing in address, assume "http" is required.
     */
    private List<String> hosts = new ArrayList<>(List.of("http://localhost:9200"));

    /**
     * elasticsearch username. If blank then no auth required
     */
    private String username;

    /**
     * elasticsearch password. If blank then no auth required
     */
    private String password;

    /**
     * Elasticsearch search scroll timeout. Default value is 10 minutes.
     *
     * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/scroll-api.html">scroll api</a>
     */
    @PositiveOrZero
    private Duration scrollTimeout = Duration.ofMinutes(10);

    /**
     * Elasticsearch search timeout. Default value is 2 minutes.
     */
    @PositiveOrZero
    private Duration searchTimeout = Duration.ofMinutes(2);

    /**
     * Bulk operation's configuration options.
     */
    @NotNull
    private BulkOptions bulk = new BulkOptions();

    /**
     * Elasticsearch rest client connection configuration options.
     */
    @NotNull
    private ConnectionOptions connection = new ConnectionOptions();

    /**
     * Index name to store definitions of libraries.
     */
    private String libraries = LIBRARIES_INDEX_NAME;

    /**
     * Maximum number of concurrent shards a search request can hit per node. <br/>
     * Improves query performance if targeting a lot of shards with enough resources available.<br/>
     * If cluster is already at capacity, running more stuff in parallel
     * doesn't optimize request nor affect cluster in a good way.
     */
    private int maxConcurrentShardRequests;

    /**
     * Bulk operation configuration options.
     */
    @Data
    public static class BulkOptions {
        /**
         * Elasticsearch bulk upload action size. Sets when to flush a new bulk request based on the number of actions
         * currently added. Defaults to 1000. Can be set to -1 to disable it.
         *
         * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html">bulk api</a>
         */
        private int actionsSize = 200;
        /**
         * Elasticsearch bulk upload number of concurrent requests allowed to be executed. A value of 0 means that only a
         * single request will be allowed to be executed. A value of 1 means 1 concurrent request is allowed to be executed
         * while accumulating new bulk requests. Defaults to 1.
         *
         * @see <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html">bulk api</a>
         */
        private int concurrency = 0;
    }

    /**
     * Http client connection options.
     */
    @Data
    public static class ConnectionOptions {
        /**
         * Elasticsearch http client socket timeout.
         */
        private Duration socketTimeout = Duration.ofMinutes(1);
        /**
         * Elasticsearch http client connection timeout.
         */
        private Duration connectionTimeout = Duration.ofMinutes(1);
    }
}
