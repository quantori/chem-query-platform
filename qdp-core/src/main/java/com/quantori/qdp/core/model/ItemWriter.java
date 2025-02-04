package com.quantori.qdp.core.model;

/**
 * An item writer to store items (molecules and reactions) to a storage.
 *
 * @param <T> type off entity to write
 */
public interface ItemWriter<T> extends AutoCloseable {

  /**
   * Write a single item to a storage.
   *
   * @param item an item to store
   */
  void write(T item);

  /**
   * Flush items from a buffer to a storage.
   */
  void flush();

  /**
   * Close an item writer and persist all items.
   */
  void close();
}
