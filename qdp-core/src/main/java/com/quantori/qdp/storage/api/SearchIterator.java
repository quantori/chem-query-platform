package com.quantori.qdp.storage.api;

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

  /**
   * Empty iterator returns just empty list.
   */
  static <V> SearchIterator<V> empty() {
    return List::of;
  }

  /**
   * Get next matched item list. If returned list is empty, then assume the search is completed. Pay attention this
   * expected to be thread safe. Code using this element expects it to be thread safe in blocking manner.
   *
   * @return list of matched items or empty list otherwise
   */
  @NotNull List<R> next();

  @Override
  default void close() throws IOException {
    //do nothing
  }
}
