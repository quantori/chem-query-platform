package com.quantori.qdp.core.source.model;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Data source produce stream of data.
 */
public interface DataSource<T> extends Closeable {

  /* data to process */
  Iterator<T> createIterator();
}
