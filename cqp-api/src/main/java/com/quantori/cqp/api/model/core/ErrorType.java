package com.quantori.cqp.api.model.core;

/**
 * Type of error occurred during a search.
 */
public enum ErrorType {
  /**
   * Error on filter step
   */
  FILTER,
  /**
   * Error on transformer step
   */
  TRANSFORMER,
  /**
   * Error in DataSearcher next()
   */
  STORAGE,
  /**
   * Other errors
   */
  GENERAL
}
