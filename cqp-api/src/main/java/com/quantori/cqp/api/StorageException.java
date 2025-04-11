package com.quantori.cqp.api;

/**
 * A generic error message that might be used and thrown by a specific implementation of a storage indicating that there
 * is an issue preventing the request to be processed.
 */
public class StorageException extends RuntimeException {
  /**
   * Constructs a {@code StorageException} with the specified detail message.
   *
   * @param message the detail message, or null
   */
  public StorageException(String message) {
    super(message);
  }

  /**
   * Constructs a {@code StorageException} as a wrapper of original error.
   *
   * @param t original error
   */
  public StorageException(Throwable t) {
    super(t);
  }

  /**
   * Constructs a {@code StorageException} with the specified detail message and error code.
   *
   * @param message   the detail message, or null
   * @param errorCode the error code that can be additionally specified as part of the message to indicate some specific
   *                  storage issue, i.e. an error code returned by a jdbc driver
   */
  public StorageException(String message, String errorCode) {
    super(String.format("%s, errorCode %s", message, errorCode));
  }

  /**
   * Constructs a {@code StorageException} with the specified detail message and cause.
   *
   * @param message the detail message, or null
   * @param cause   the cause
   */
  public StorageException(String message, Throwable cause) {
    super(message, cause);
  }
}
