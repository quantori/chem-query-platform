package com.quantori.cqp.api.model;

/**
 * List of supported library types.
 */
public enum LibraryType {
  /**
   * Molecules structure library.
   */
  molecules,
  /**
   * Reactions structures library.
   */
  reactions,
  /**
   * Metrics libraries (for internal usage).
   */
  metrics,
  /**
   * Library with arbitrary data.
   */
  any
}
