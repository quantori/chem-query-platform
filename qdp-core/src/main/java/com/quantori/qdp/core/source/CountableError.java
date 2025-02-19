package com.quantori.qdp.core.source;

import lombok.Getter;

/** Wrapper exception to preserve index of item where error occurred. */
@Getter
public class CountableError extends RuntimeException {
  private final Long at;
  private final int successful;

  public CountableError(final Long at, final int successful, final Throwable e) {
    super(e);
    this.successful = successful;
    this.at = at;
  }
}
