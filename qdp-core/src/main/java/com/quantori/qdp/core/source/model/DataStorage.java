package com.quantori.qdp.core.source.model;

import java.util.List;

public interface DataStorage<I> {
  default DataLoader<I> dataLoader(String libraryName) {
    throw new UnsupportedOperationException();
  }

  default List<DataLibrary> getLibraries() {
    throw new UnsupportedOperationException();
  }

  default DataLibrary createLibrary(DataLibrary library) {
    throw new UnsupportedOperationException();
  }

  default DataLibrary findLibrary(String libraryName, DataLibraryType libraryType) {
    throw new UnsupportedOperationException();
  }

  default DataSearcher dataSearcher(RequestStructure storageRequest) {
    throw new UnsupportedOperationException();
  }
}
