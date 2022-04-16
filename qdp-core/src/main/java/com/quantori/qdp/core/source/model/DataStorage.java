package com.quantori.qdp.core.source.model;

import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import java.util.List;

public interface DataStorage<T> {
  default DataLoader<T> dataLoader(String libraryName) {
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

  default DataSearcher dataSearcher(SearchRequest searchRequest) {
    throw new UnsupportedOperationException();
  }
}
