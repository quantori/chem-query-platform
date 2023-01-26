package com.quantori.qdp.api.model.core;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class SearchRequest<S extends SearchItem, I extends StorageItem> {
  public static final int DEFAULT_BUFFER_SIZE = 1000;
  public static final int DEFAULT_PARALLELISM = 2;
  @Builder.Default
  private final int bufferSize = DEFAULT_BUFFER_SIZE;
  @Builder.Default
  private final int parallelism = DEFAULT_PARALLELISM;
  @Builder.Default
  private final FetchWaitMode fetchWaitMode = FetchWaitMode.WAIT_COMPLETE;
  @Builder.Default
  private final boolean runCountTask = false;
  private final String user;
  private final Map<String, StorageRequest> requestStorageMap;
  private final Predicate<I> resultFilter;
  private final Function<I, S> resultTransformer;
}
