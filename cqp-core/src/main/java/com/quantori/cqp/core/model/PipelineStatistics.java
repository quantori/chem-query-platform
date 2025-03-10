package com.quantori.cqp.core.model;

import lombok.Value;

@Value
public class PipelineStatistics {

  int countOfSuccessfullyProcessed;
  int countOfErrors;

  public boolean isFailed() {
    return countOfErrors != 0;
  }
}
