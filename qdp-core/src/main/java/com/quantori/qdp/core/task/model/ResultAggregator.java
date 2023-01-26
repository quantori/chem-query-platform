package com.quantori.qdp.core.task.model;

public interface ResultAggregator {
    void consume(DataProvider.Data data);

    StreamTaskResult getResult();

    double getPercent();

    default void close() { }

    default void taskCompleted(boolean successful) { }
}
