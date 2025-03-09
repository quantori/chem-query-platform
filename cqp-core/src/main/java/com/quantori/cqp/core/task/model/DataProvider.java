package com.quantori.cqp.core.task.model;

import com.quantori.cqp.core.task.actor.TaskFlowActor;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public interface DataProvider {
  Data EMPTY_DATA = new Data() {};
  DataProvider EMPTY = Collections::emptyIterator;

  /**
   * Data provider with a single dummy empty element of type {@link Data}. Only needed to trigger
   * task execution so processor will hit aggregator.
   *
   * @see TaskFlowActor
   */
  static DataProvider single() {
    return () -> List.of(EMPTY_DATA).iterator();
  }

  Iterator<? extends Data> dataIterator();

  default void close() {}

  default void taskCompleted(boolean successful) {}

  interface Data {}
}
