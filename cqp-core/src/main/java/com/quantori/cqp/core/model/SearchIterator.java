package com.quantori.cqp.core.model;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import javax.validation.constraints.NotNull;

/**
 * Iterator fetches list of items(batch) on each iteration.
 *
 * @author simar.mugattarov
 */
public interface SearchIterator<R> extends Closeable {

  /** Empty iterator returns just empty list. */
  static <R> SearchIterator<R> empty(String storageName) {
    return new SearchIterator<R>() {
      @Override
      public List<R> next() {
        return List.of();
      }

      @Override
      public String getStorageName() {
        return storageName;
      }

      @Override
      public List<String> getLibraryIds() {
        return List.of();
      }
    };
  }

  /**
   * Get next matched item list. If returned list is empty, then assume the search is completed. Pay
   * attention this expected to be thread safe. Code using this element expects it to be thread safe
   * in blocking manner.
   *
   * @return a list of matched items or empty list otherwise
   */
  @NotNull
  List<R> next();

  /**
   * Closes an iterator.
   *
   * <p>The default implementation does nothing.
   *
   * @throws IOException In case an iterator cannot be closed, {@code IOException} must be thrown.
   */
  @Override
  default void close() throws IOException {}

  String getStorageName();

  List<String> getLibraryIds();
}
