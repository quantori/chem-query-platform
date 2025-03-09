package com.quantori.cqp.core.task.model;

import java.util.Map;
import java.util.function.Consumer;

public interface FlowFinalizer extends Consumer<Boolean> {

  FlowFinalizerSerDe getSerializer();

  default Map<String, String> getParameters() {
    return Map.of();
  }
}
