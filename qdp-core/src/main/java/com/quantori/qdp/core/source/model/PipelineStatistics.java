package com.quantori.qdp.core.source.model;

import lombok.Value;

@Value
public class PipelineStatistics {

  int countOfSuccessfullyProcessed;
  int countOfErrors;

  public boolean isFailed() {
    return countOfErrors != 0;
  }
}
