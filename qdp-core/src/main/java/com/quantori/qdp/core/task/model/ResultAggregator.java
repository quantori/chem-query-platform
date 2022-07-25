package com.quantori.qdp.core.task.model;

public interface ResultAggregator {
    void consume(DataProvider.Data data);

    StreamTaskResult getResult();

    float getPercent();

    default void close() { }

    default void taskCompleted(boolean successful) { }
}
