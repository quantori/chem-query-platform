package com.quantori.qdp.core.source.model;

import java.util.List;

public interface DataStorage<I> {
  default DataLoader<I> dataLoader(String libraryId) {
    throw new UnsupportedOperationException();
  }

  default <S extends SearchItem> List<DataSearcher> dataSearcher(RequestStructure<S> storageRequest) {
    throw new UnsupportedOperationException();
  }
}
