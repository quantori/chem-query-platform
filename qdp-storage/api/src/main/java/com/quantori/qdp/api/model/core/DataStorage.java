package com.quantori.qdp.api.model.core;

import com.quantori.qdp.api.service.ItemWriter;
import com.quantori.qdp.api.service.SearchIterator;
import java.util.List;

public interface DataStorage<U extends StorageUploadItem, S extends SearchItem, I extends StorageItem> {
  default ItemWriter<U> itemWriter(String libraryId) {
    throw new UnsupportedOperationException();
  }

  default List<SearchIterator<I>> searchIterator(RequestStructure<S, I> storageRequest) {
    throw new UnsupportedOperationException();
  }
}
