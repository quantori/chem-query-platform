package com.quantori.qdp.api.model;

/**
 * Type of libraries bingo supports.
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
