package com.quantori.cqp.api.model;

/**
 * List of supported library types.
 */
public enum LibraryType {
  /**
   * Molecules structure library.
   */
  MOLECULES,
  /**
   * Reactions structures library.
   */
  REACTIONS,
  /**
   * Metrics libraries (for internal usage).
   */
  METRICS,
  /**
   * Library with arbitrary data.
   */
  ANY
}
