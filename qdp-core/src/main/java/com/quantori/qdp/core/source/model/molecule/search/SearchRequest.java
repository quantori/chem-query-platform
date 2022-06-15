package com.quantori.qdp.core.source.model.molecule.search;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class SearchRequest {
  /**
   * PAGE_BY_PAGE - strategy which fetches data synchronously with user request but with buffering
   * PAGE_FROM_STREAM - strategy which fetches data asynchronously with user request. It works in background after user request is completed.
   */
  public enum SearchStrategy {
    PAGE_BY_PAGE,
    PAGE_FROM_STREAM
  }

  /**
   * NO_WAIT - result of search can have fewer data then requested. Returns data from the buffer without waiting. User have to request again to fetch more data.
   * Works only in combination with  PAGE_FROM_STREAM strategy.
   * <p>
   * WAIT_COMPLETE - user will wait until all data will be fetched according to provided limit parameter. It will return fewer data then requested only when search is completed.
   */
  public enum WaitMode {
    NO_WAIT,
    WAIT_COMPLETE
  }

  public interface Request {
  }

  public interface StorageResultItem {
  }

  //TODO: extract processing settings to separate class/interface (maybe, together with transformation step)
  public static final int DEFAULT_BUFFER_SIZE = 1000;
  public static final int DEFAULT_PARALLELISM = 1;

  private final String user;

  private final String storageName;
  private final List<String> indexNames;
  private final int pageSize;
  private final int hardLimit;
  private final SearchStrategy strategy;
  private final WaitMode waitMode;

  @Builder.Default
  private final int bufferSize = DEFAULT_BUFFER_SIZE;
  @Builder.Default
  private final int parallelism = DEFAULT_PARALLELISM;

  private final Request storageRequest;
  private final Predicate<StorageResultItem> resultFilter;
  private final Function<StorageResultItem, SearchResultItem> resultTransformer;
  private final Map<String, String> propertyTypes;
}
