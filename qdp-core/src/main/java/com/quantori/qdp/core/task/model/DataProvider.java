package com.quantori.qdp.core.task.model;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public interface DataProvider {

    DataProvider EMPTY = Collections::emptyIterator;

    Iterator<? extends Data> dataIterator();

    default void close() { }

    default void taskCompleted(boolean successful) { }

    interface Data { }
}
