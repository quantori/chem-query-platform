package com.quantori.qdp.core.task.model;

import java.util.Map;

public interface FlowFinalizerSerDe {

    String serialize(Map<String, String> params);

    FlowFinalizer deserialize(String data);
}
