package com.quantori.qdp.api.model.core;

import java.util.List;

public interface DataStorage<U extends StorageUploadItem, S extends SearchItem, I extends StorageItem> {
  default DataLoader<U> dataLoader(String libraryId) {
    throw new UnsupportedOperationException();
  }

  default List<DataSearcher<I>> dataSearcher(RequestStructure<S, I> storageRequest) {
    throw new UnsupportedOperationException();
  }
}
