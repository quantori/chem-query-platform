package com.quantori.cqp.core.task.model;

import java.util.Map;

public interface FlowFinalizerSerDe {

  String serialize(Map<String, String> params);

  FlowFinalizer deserialize(String data);

  void setRequiredEntities(Object entityHolder);
}
