package com.quantori.cqp.api.indigo;

public class ObjectPoolException extends RuntimeException {

  public ObjectPoolException() {
  }

  public ObjectPoolException(String message) {

    super(message);
  }

  public ObjectPoolException(String message, Throwable cause) {

    super(message, cause);
  }
}
