package com.quantori.qdp.core.source.model.molecule.search;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

public class SearchRequest {
  public enum SearchStrategy {
    PAGE_BY_PAGE,
    PAGE_FROM_STREAM
  }

  public enum WaitMode {
    NO_WAIT,
    WAIT_COMPLETE
  }

  public interface Request {}

  public interface StorageResultItem {}

  private final String storageName;
  private final List<String> indexNames;
  private final int pageSize;
  private final int hardLimit;
  private final SearchStrategy strategy;
  private final WaitMode waitMode;

  //TODO: extract processing settings to separate class/interface (maybe, together with transformation step)
  public static final int DEFAULT_BUFFER_SIZE = 1000;
  private final int bufferSize;

  public static final int DEFAULT_PARALLELISM = 1;
  private final int parallelism;

  private final Request storageRequest;
  private final Predicate<StorageResultItem> resultFilter;
  private final Function<StorageResultItem, SearchResultItem> resultTransformer;
  private final Map<String, String> propertyTypes;

  private SearchRequest(String storageName, List<String> indexNames, int pageSize, int hardLimit,
                        SearchStrategy strategy,
                        WaitMode waitMode,
                        Request storageRequest,
                        Predicate<StorageResultItem> resultFilter,
                        Function<StorageResultItem, SearchResultItem> resultTransformer,
                        int bufferSize,
                        int parallelism,
                        Map<String, String> propertyTypes) {
    this.storageName = storageName;
    this.indexNames = indexNames;
    this.pageSize = pageSize;
    this.hardLimit = hardLimit;
    this.strategy = strategy;
    this.waitMode = waitMode;
    this.resultFilter = resultFilter;
    this.storageRequest = storageRequest;
    this.resultTransformer = resultTransformer;
    this.bufferSize = bufferSize;
    this.parallelism = parallelism;
    this.propertyTypes = propertyTypes;
  }

  public String getStorageName() {
    return storageName;
  }

  public String getIndexName() {
    return indexNames.stream().findFirst().orElse(null);
  }

  public List<String> getIndexNames() {
    return indexNames;
  }

  public int getPageSize() {
    return pageSize;
  }

  public int getHardLimit() {
    return hardLimit;
  }

  public SearchStrategy getStrategy() {
    return strategy;
  }

  public WaitMode getWaitMode() {
    return waitMode;
  }

  public Request getStorageRequest() {
    return storageRequest;
  }

  public Predicate<StorageResultItem> getResultFilter() {
    return resultFilter;
  }

  public Function<StorageResultItem, SearchResultItem> getResultTransformer() {
    return resultTransformer;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public int getParallelism() {
    return parallelism;
  }

  public Map<String, String> getPropertyTypes() {
    return propertyTypes;
  }

  public static class Builder {
    private String storageName;
    private List<String> indexNames;
    private int pageSize;
    private int hardLimit;
    private SearchStrategy strategy;
    private WaitMode waitMode;
    private Predicate<StorageResultItem> resultFilter;
    private Function<StorageResultItem, SearchResultItem> resultTransformer;
    private Request storageRequest;
    private Map<String, String> propertyTypes;
    private int bufferSize = DEFAULT_BUFFER_SIZE;
    private int parallelism = DEFAULT_PARALLELISM;

    public Builder storageName(String storageName) {
      this.storageName = storageName;
      return this;
    }

    public Builder indexName(String indexName) {
      this.indexNames = List.of(indexName);
      return this;
    }

    public Builder indexNames(List<String> indexNames) {
      this.indexNames = indexNames;
      return this;
    }

    public Builder pageSize(int pageSize) {
      this.pageSize = pageSize;
      return this;
    }

    public Builder hardLimit(int hardLimit) {
      this.hardLimit = hardLimit;
      return this;
    }

    public Builder strategy(SearchStrategy strategy) {
      this.strategy = strategy;
      return this;
    }

    public Builder waitMode(WaitMode waitMode) {
      this.waitMode = waitMode;
      return this;
    }

    public Builder resultFilter(Predicate<StorageResultItem> resultFilter) {
      this.resultFilter = resultFilter;
      return this;
    }

    public Builder request(Request storageRequest) {
      this.storageRequest = storageRequest;
      return this;
    }

    public Builder resultTransformer(Function<StorageResultItem, SearchResultItem> resultTransformer) {
      this.resultTransformer = resultTransformer;
      return this;
    }

    public Builder bufferSize(int bufferSize) {
      this.bufferSize = bufferSize;
      return this;
    }

    public Builder parallelism(int parallelism) {
      this.parallelism = parallelism;
      return this;
    }

    public Builder propertyTypes(Map<String, String> propertyTypes) {
      this.propertyTypes = propertyTypes;
      return this;
    }

    public SearchRequest build() {
      return new SearchRequest(storageName, indexNames, pageSize, hardLimit, strategy, waitMode, storageRequest,
          resultFilter, resultTransformer, bufferSize, parallelism, propertyTypes);
    }
  }
}
