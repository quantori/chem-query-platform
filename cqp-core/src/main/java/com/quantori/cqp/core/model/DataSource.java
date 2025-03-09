package com.quantori.cqp.core.model;

import java.io.Closeable;
import java.util.Iterator;

/** Data source produce stream of data. */
public interface DataSource<D extends DataUploadItem> extends Closeable {

  /* data to process */
  Iterator<D> createIterator();
}
