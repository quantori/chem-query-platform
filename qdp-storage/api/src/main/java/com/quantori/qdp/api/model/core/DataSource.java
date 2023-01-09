package com.quantori.qdp.api.model.core;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Data source produce stream of data.
 */
public interface DataSource<D extends DataUploadItem> extends Closeable {

  /* data to process */
  Iterator<D> createIterator();
}
