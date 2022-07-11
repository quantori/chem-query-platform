package com.quantori.qdp.core.source.model;

import java.util.List;

public interface DataLoader extends AutoCloseable {
  default void add(StorageItem storageItem) {
    throw new UnsupportedOperationException();
  }

  default void addBatch(List<StorageItem> storageItems) {
    throw new UnsupportedOperationException();
  }

  @Override
  default void close() throws Exception {
    throw new UnsupportedOperationException();
  }
}
