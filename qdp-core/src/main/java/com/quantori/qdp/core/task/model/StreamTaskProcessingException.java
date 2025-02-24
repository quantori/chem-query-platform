package com.quantori.qdp.core.task.model;

public class StreamTaskProcessingException extends RuntimeException {
  public StreamTaskProcessingException(String message) {
    super(message);
  }

  public StreamTaskProcessingException(String message, Throwable cause) {
    super(message, cause);
  }
}
