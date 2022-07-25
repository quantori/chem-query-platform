package com.quantori.qdp.core.task.model;

import java.util.Iterator;
import java.util.List;

public interface DataProvider {
    Data EMPTY_DATA = new Data() { };

    DataProvider EMPTY = () -> List.of(EMPTY_DATA).iterator();

    Iterator<? extends Data> dataIterator();

    default void close() { }

    default void taskCompleted(boolean successful) { }

    interface Data { }
}
