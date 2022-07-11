package com.quantori.qdp.core.source;

import com.quantori.qdp.core.source.model.DataSearcher;
import com.quantori.qdp.core.source.model.MultiStorageSearchRequest;
import com.quantori.qdp.core.source.model.SearchResult;
import com.quantori.qdp.core.source.model.SearchItem;
import com.quantori.qdp.core.source.model.StorageItem;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SearchByPage implements Searcher {
  private final String user;
  private final Map<String, DataSink> dataStructures;
  private final String searchId;

  private final LinkedBlockingDeque<SearchItem> buffer = new LinkedBlockingDeque<>();

  private final AtomicLong errorCount = new AtomicLong(0);
  private final AtomicLong foundCount = new AtomicLong(0);
  private final AtomicLong matchedCount = new AtomicLong(0);
  private boolean finished;

  public SearchByPage(Map<String, DataSearcher> dataSearchers, MultiStorageSearchRequest searchRequest,
                      String searchId) {
    this.dataStructures = searchRequest.getRequestStorageMap().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> new DataSink(
            new AtomicBoolean(false),
            new AtomicBoolean(false),
            dataSearchers.get(entry.getKey()),
            entry.getValue().getResultFilter(),
            entry.getValue().getResultTransformer()
        )));
    this.searchId = searchId;
    user = searchRequest.getProcessingSettings().getUser();
  }

  public CompletionStage<SearchResult> searchNext(final int limit) {
    if (!finished) {
      fillBuffer(limit);
    }

    List<SearchItem> searchResults = new ArrayList<>();
    buffer.drainTo(searchResults, limit);

    return CompletableFuture.completedFuture(SearchResult.builder()
        .searchId(searchId)
        .searchFinished(finished)
        .foundCount(foundCount.get())
        .matchedByFilterCount(matchedCount.get())
        .errorCount(errorCount.get())
        .results(searchResults)
        .build());
  }

  @Override
  public void close() {
    dataStructures.values().stream()
        .map(DataSink::getDataSearcher)
        .forEach(dataSearcher -> {
          try {
            dataSearcher.close();
          } catch (Exception e) {
            log.error("Failed to close data searcher", e);
          }
        });
    buffer.clear();
    finished = true;
  }

  @Override
  public String getUser() {
    return user;
  }

  private void fillBuffer(int limit) {
    while (buffer.size() < limit && dataStructures.values().stream().anyMatch(dataSink -> !dataSink.closed.get())) {
      List<CompletableFuture<Boolean>> results = runGetSearchResults();
      CompletableFuture.allOf(results.toArray(CompletableFuture[]::new))
          .thenApply(unused -> results.stream().allMatch(CompletableFuture::join))
          .toCompletableFuture()
          .completeOnTimeout(true, 100, TimeUnit.MILLISECONDS);
    }
    if(dataStructures.values().stream().allMatch(dataSink -> dataSink.closed.get())) {
      finished = true;
    }
  }

  private List<CompletableFuture<Boolean>> runGetSearchResults() {
    return dataStructures.values().stream()
        .filter(dataSink -> !dataSink.closed.get())
        .filter(dataSink -> !dataSink.running.get())
        .map(dataSink -> {
          dataSink.running.set(true);
          List<? extends StorageItem> storageResultItems = dataSink.getDataSearcher().next();
          if (storageResultItems.isEmpty()) {
            dataSink.closed.set(true);
            dataSink.running.set(false);
            return CompletableFuture.completedFuture(true);
          }
          List<SearchItem> items = storageResultItems.stream()
              .peek(item -> foundCount.incrementAndGet())
              .filter(item -> filter(item, dataSink.resultFilter))
              .map(item -> transform(item, dataSink.resultTransformer))
              .filter(Objects::nonNull)
              .peek(item -> matchedCount.incrementAndGet())
              .collect(Collectors.toList());
          buffer.addAll(items);
          dataSink.running.set(false);
          return CompletableFuture.completedFuture(true);
        }).toList();
  }

  private boolean filter(StorageItem item, Predicate<StorageItem> resultFilter) {
    try {
      return resultFilter.test(item);
    } catch (Exception e) {
      log.error("Failed to filter item {}", item, e);
      errorCount.incrementAndGet();
    }
    return false;
  }

  private SearchItem transform(StorageItem item, Function<StorageItem, SearchItem> resultTransformer) {
    try {
      return resultTransformer.apply(item);
    } catch (Exception e) {
      log.error("Failed to transform item {}", item, e);
      errorCount.incrementAndGet();
    }
    return null;
  }

  @Value
  private static class DataSink {
    AtomicBoolean running;
    AtomicBoolean closed;
    DataSearcher dataSearcher;
    Predicate<StorageItem> resultFilter;
    Function<StorageItem, SearchItem> resultTransformer;
  }
}
