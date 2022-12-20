package com.quantori.qdp.api.model;

/**
 * An error message to indicate that a requested to be created library already exists. The API does not add any
 * restrictions to libraries but particular storages might require libraries to unique by name, or by name and type,
 * etc.
 */
public class LibraryAlreadyExistsException extends RuntimeException {
  /**
   * Constructs a {@code LibraryAlreadyExistsException} with the specified storage type and library name.
   *
   * @param storageType a storage type
   * @param libraryName a library name
   */
  public LibraryAlreadyExistsException(String storageType, String libraryName) {
    super(String.format("Library %s already exists in storage %s", libraryName, storageType));
  }
}
