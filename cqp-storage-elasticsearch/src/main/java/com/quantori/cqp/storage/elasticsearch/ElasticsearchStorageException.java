package com.quantori.cqp.storage.elasticsearch;

import com.quantori.cqp.api.StorageException;
import lombok.Getter;

@Getter
public class ElasticsearchStorageException extends StorageException {

  private final String errorCode;

  public ElasticsearchStorageException(String message) {
    super(message);
    this.errorCode = null;
  }

  public ElasticsearchStorageException(String message, String code) {
    super(message, code);
    this.errorCode = code;
  }

  public ElasticsearchStorageException(String message, int code){
    this(message, Integer.toString(code));
  }

  public ElasticsearchStorageException(String message, Throwable cause) {
    super(message, cause);
    this.errorCode = null;
  }
}
