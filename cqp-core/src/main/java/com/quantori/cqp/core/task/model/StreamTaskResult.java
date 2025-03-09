package com.quantori.cqp.core.task.model;

import java.util.List;

public interface StreamTaskResult {

  StreamTaskResult EMPTY = new StreamTaskResult() {};

  default List<String> messages() {
    return List.of();
  }
}
