package com.quantori.qdp.core.source.model;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class ProcessingSettings {
  public static final int DEFAULT_BUFFER_SIZE = 1000;
  public static final int DEFAULT_PARALLELISM = 1;
  @Builder.Default
  private final int bufferSize = DEFAULT_BUFFER_SIZE;
  @Builder.Default
  private final int parallelism = DEFAULT_PARALLELISM;
  private final String user;
  private final FetchWaitMode fetchWaitMode;
  private final boolean runCountTask;
}
