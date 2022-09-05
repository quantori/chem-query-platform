package com.quantori.qdp.core.source.model;

import java.util.List;

public interface DataSearcher extends AutoCloseable {
  List<? extends StorageItem> next();

  String getStorageName();

  List<String> getLibraryIds();
}
