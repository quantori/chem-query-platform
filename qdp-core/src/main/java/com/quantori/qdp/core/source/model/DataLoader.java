package com.quantori.qdp.core.source.model;

import java.util.List;

public interface DataLoader<T> extends AutoCloseable {
  default void add(T dataItem) {
    throw new UnsupportedOperationException();
  }

  default void addBatch(List<T> dataItems) {
    throw new UnsupportedOperationException();
  }

  @Override
  default void close() throws Exception {
    throw new UnsupportedOperationException();
  }
}
