package com.quantori.qdp.api.model;

public class LibraryAlreadyExistsException extends RuntimeException {
  public LibraryAlreadyExistsException(String storageType, String libraryName) {
    super(String.format("Library %s already exists in storage %s", libraryName, storageType));
  }
}
