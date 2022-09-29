package com.quantori.qdp.storage.api;

public class StorageException extends RuntimeException {
  public StorageException(String message) {
    super(message);
  }

  public StorageException(String message, String errorCode) {
    super(String.format("%s, errorCode %s", message, errorCode));
  }

  public StorageException(String message, Throwable cause) {
    super(message, cause);
  }
}
