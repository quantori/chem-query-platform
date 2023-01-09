package com.quantori.qdp.api.model.core;

import java.util.List;

public interface DataLoader<U extends StorageUploadItem> extends AutoCloseable {
  default void add(U storageItem) {
    throw new UnsupportedOperationException();
  }

  default void addBatch(List<U> storageItems) {
    throw new UnsupportedOperationException();
  }

  @Override
  default void close() throws Exception {
    throw new UnsupportedOperationException();
  }
}
