package com.quantori.qdp.core.task.model;

public interface SimpleResultAggregator extends ResultAggregator {

  default void consume(DataProvider.Data data) { }

  StreamTaskResult getResult();

  default float getPercent() {
    return 100;
  }
}
