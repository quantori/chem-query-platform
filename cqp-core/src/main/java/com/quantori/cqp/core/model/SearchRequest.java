package com.quantori.cqp.core.model;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

import com.quantori.cqp.api.SearchItem;
import com.quantori.cqp.api.StorageItem;
import com.quantori.cqp.api.model.StorageRequest;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class SearchRequest<S extends SearchItem, I extends StorageItem> {
  public static final int DEFAULT_BUFFER_SIZE = 1000;
  public static final int DEFAULT_FETCH_LIMIT = 1000;
  public static final int DEFAULT_PARALLELISM = 2;
  @Builder.Default private final int bufferSize = DEFAULT_BUFFER_SIZE;
  @Builder.Default private final int fetchLimit = DEFAULT_FETCH_LIMIT;
  @Builder.Default private final int parallelism = DEFAULT_PARALLELISM;
  @Builder.Default private final FetchWaitMode fetchWaitMode = FetchWaitMode.WAIT_COMPLETE;
  @Builder.Default private final boolean isCountTask = false;
  private final String user;
  private final Map<String, StorageRequest> requestStorageMap;
  private final Predicate<I> resultFilter;
  private final Function<I, S> resultTransformer;
}
