package com.quantori.qdp.api.model.core;

import java.util.List;

public interface DataSearcher extends AutoCloseable {
  List<? extends StorageItem> next();

  String getStorageName();

  List<String> getLibraryIds();
}
