package com.quantori.qdp.storage.solr;

import com.quantori.qdp.storage.api.StorageException;

public class SolrStorageException extends StorageException {
  public SolrStorageException(String message) {
    super(message);
  }

  public SolrStorageException(String message, String errorCode) {
    super(message, errorCode);
  }

  public SolrStorageException(String message, Throwable cause) {
    super(message, cause);
  }
}
