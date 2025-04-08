package com.quantori.cqp.api.service;

/**
 * An item writer to store items (molecules and reactions) to a storage.
 *
 * @param <T> currently {@link com.quantori.qdp.api.model.upload.Molecule} and
 *            {@link com.quantori.qdp.api.model.upload.Reaction} are supported as parametrized type
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
