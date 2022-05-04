package com.quantori.qdp.core.source.external;

import com.quantori.qdp.core.source.model.DataSearcher;
import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import com.quantori.qdp.core.source.model.molecule.search.SearchResult;
import com.quantori.qdp.core.source.model.molecule.search.SearchResultItem;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SearchByPage implements Searcher {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final SearchRequest searchRequest;
  private final DataSearcher dataSearcher;
  private final String searchId;

  private final LinkedList<SearchResultItem> buffer = new LinkedList<>();

  private long errorCounter;
  private long foundByStorageCount;
  private long matchedCount;
  private boolean finished;

  public SearchByPage(SearchRequest searchRequest, DataSearcher dataSearcher, String searchId) {
    this.searchRequest = searchRequest;
    this.dataSearcher = dataSearcher;
    this.searchId = searchId;
  }

  public SearchResult searchNext(final int limit) {
    if (!finished) {
      fillBuffer(limit);
    }

    List<SearchResultItem> searchResults = drainBuffer(limit);

    return SearchResult.builder()
        .searchId(searchId)
        .searchFinished(finished)
        .foundByStorageCount(foundByStorageCount)
        .matchedByFilterCount(matchedCount)
        .errorCount(errorCounter)
        .results(searchResults)
        .build();
  }

  public SearchResult searchStat() {
    return SearchResult.builder()
        .searchId(searchId)
        .searchFinished(finished)
        .foundByStorageCount(foundByStorageCount)
        .matchedByFilterCount(matchedCount)
        .errorCount(errorCounter)
        .build();
  }

  @Override
  public void close() {
    try {
      dataSearcher.close();
    } catch (Exception e) {
      logger.error("Failed to close data searcher", e);
    }
    buffer.clear();
    finished = true;
  }

  @Override
  public SearchRequest getSearchRequest() {
    return searchRequest;
  }

  private List<SearchResultItem> drainBuffer(int limit) {
    var searchResults = new ArrayList<SearchResultItem>();
    if (finished || buffer.size() <= limit) {
      searchResults.addAll(buffer);
      buffer.clear();
    } else {
      while (searchResults.size() < limit && !buffer.isEmpty()) {
        searchResults.add(buffer.poll());
      }
    }
    return searchResults;
  }

  private void fillBuffer(int limit) {
    while (buffer.size() < limit) {
      List<? extends SearchRequest.StorageResultItem> storageResultItems = dataSearcher.next();
      if (storageResultItems.isEmpty()) {
        finished = true;
        break;
      }

      List<SearchResultItem> items = storageResultItems.stream()
          .peek(item -> foundByStorageCount++)
          .filter(res -> filter(searchRequest.getResultFilter(), res))
          .map(res -> transform(searchRequest.getResultTransformer(), res))
          .filter(Objects::nonNull)
          .peek(item -> matchedCount++)
          .collect(Collectors.toList());
      buffer.addAll(items);
    }
  }

  private <T> boolean filter(Predicate<T> filter, T item) {
    try {
      return filter.test(item);
    } catch (RuntimeException e) {
      logger.error("Failed to filter item {}", item, e);
      errorCounter++;
    }
    return false;
  }

  private <I, O> O transform(Function<I, O> function, I item) {
    try {
      return function.apply(item);
    } catch (RuntimeException e) {
      logger.error("Failed to transform item {}", item, e);
      errorCounter++;
    }
    return null;
  }
}
