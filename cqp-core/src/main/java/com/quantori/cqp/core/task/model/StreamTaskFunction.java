package com.quantori.cqp.core.task.model;

public interface StreamTaskFunction {
  DataProvider.Data apply(DataProvider.Data data);

  static StreamTaskFunction identity() {
    return data -> data;
  }
}
