package com.quantori.cqp.storage.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.JsonpMapper;
import jakarta.json.spi.JsonProvider;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.Flushable;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;

@Slf4j
class BulkProcessorItemWriter implements Closeable, Flushable {
  private final ElasticsearchAsyncClient client;
  private final Runnable onClose;
  private final ArrayBlockingQueue<BulkOperation> operations;
  private final ElasticsearchProperties.BulkOptions bulkOptions;

  BulkProcessorItemWriter(ElasticsearchAsyncClient client, Runnable onClose) {
    this.client = Objects.requireNonNull(client);
    this.onClose = onClose;
    this.bulkOptions = new ElasticsearchProperties.BulkOptions();
    this.operations = new ArrayBlockingQueue<>(bulkOptions.getActionsSize() + 10);
  }

  public void write(BulkOperation operation) {
    operations.add(operation);
    if (operations.size() >= bulkOptions.getActionsSize()) {
      flush();
    }
  }

  @Override
  public void flush() {
    List<BulkOperation> drainedOperations = new ArrayList<>();
    if (operations.drainTo(drainedOperations) != 0) {
      log.debug("Bulk operations size {}", drainedOperations.size());
      client.bulk(req -> req
        .operations(drainedOperations)
        .refresh(Refresh.WaitFor)
      ).whenComplete((bulkResponse, throwable) -> {
        if (throwable != null) {
          log.warn("Error during bulk write", throwable);
        } else {
          log.debug("Bulk response, {}", bulkResponse);
        }
      }).join();
    }
  }

  @Override
  public void close() {
    try {
      flush();
      log.debug("BulkProcessorItemWriter flush successful");
    } catch (Exception e) {
      log.warn("Failed to flush processor", e);
    }
    onClose.run();
  }

  protected JsonData readJson(Reader reader) {
    JsonpMapper jsonpMapper = client._transport().jsonpMapper();
    JsonProvider jsonProvider = jsonpMapper.jsonProvider();
    return JsonData.from(jsonProvider.createParser(reader), jsonpMapper);
  }
}
