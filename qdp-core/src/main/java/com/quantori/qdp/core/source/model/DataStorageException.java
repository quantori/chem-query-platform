package com.quantori.qdp.core.source.model;

import java.io.IOException;

public class DataStorageException extends RuntimeException {
  public DataStorageException(String message) {
    super(message);
  }

  public DataStorageException(final String message, final IOException e) {
    super(message, e);
  }
}
