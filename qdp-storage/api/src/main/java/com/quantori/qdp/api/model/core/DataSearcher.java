package com.quantori.qdp.api.model.core;

import java.util.List;

public interface DataSearcher<I extends StorageItem> extends AutoCloseable {
  List<I> next();

  String getStorageName();

  List<String> getLibraryIds();
}
