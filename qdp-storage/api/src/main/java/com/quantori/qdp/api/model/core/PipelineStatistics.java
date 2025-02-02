package com.quantori.qdp.api.model.core;

import lombok.Value;

@Value
public class PipelineStatistics {

  int countOfSuccessfullyProcessed;
  int countOfErrors;

  public boolean isFailed() {
    return countOfErrors != 0;
  }
}
