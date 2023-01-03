package com.quantori.qdp.api.model.core;

import java.util.List;

public interface DataLoader<I> extends AutoCloseable {
  default void add(I storageItem) {
    throw new UnsupportedOperationException();
  }

  default void addBatch(List<I> storageItems) {
    throw new UnsupportedOperationException();
  }

  @Override
  default void close() throws Exception {
    throw new UnsupportedOperationException();
  }
}
