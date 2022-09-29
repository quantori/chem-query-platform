package com.quantori.qdp.storage.api;

public class LibraryAlreadyExistsException extends RuntimeException {
  public LibraryAlreadyExistsException(StorageType storageType, String libraryName) {
    super(String.format("Library %s already exists in storage %s", libraryName, storageType.name()));
  }
}
