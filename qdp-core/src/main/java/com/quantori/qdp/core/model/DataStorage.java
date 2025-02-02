package com.quantori.qdp.core.model;

import java.util.List;

public interface DataStorage<U extends StorageUploadItem, I extends StorageItem> {
  /**
   * Creates a molecules/reaction writer that is used to persist items to a storage.
   * <p>
   * For more details see {@link ItemWriter}
   *
   * @param libraryId an id of a library to which molecules will be persisted
   * @return an instance of item writer
   */
  ItemWriter<U> itemWriter(String libraryId);

  List<SearchIterator<I>> searchIterator(StorageRequest storageRequest);
}
