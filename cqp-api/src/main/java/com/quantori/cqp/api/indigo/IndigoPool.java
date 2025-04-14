package com.quantori.cqp.api.indigo;

import com.epam.indigo.Indigo;
import com.quantori.cqp.api.indigo.exception.ObjectPoolException;

import java.util.concurrent.TimeUnit;

/**
 * Pool of Indigo objects.
 */
public class IndigoPool extends ObjectPool<Indigo> {

  /**
   * Creates a new Indigo pool.
   * Initializes the Indigo pool on application startup.
   */
  IndigoPool(int maximumPoolSize) {
    super(maximumPoolSize, IndigoFactory::createNewIndigo, Indigo::getSid);
  }

  /**
   * Acquires an Indigo object from the pool. Blocks and waits until an Indigo object
   * is available for the provided time. If the poll method returns a null value or the replace
   * method returns false, it throws an ObjectPoolException.
   *
   * @param timeout how long to wait before giving up, in units of {@code unit}
   * @param unit a {@code TimeUnit} determining how to interpret the {@code timeout} parameter
   *
   * @return An Indigo object from the pool.
   *
   * @throws InterruptedException if interrupted while waiting to acquire an Indigo object.
   * @throws ObjectPoolException if the Indigo pool and the indigo availability map are out of sync.
   * @deprecated use {@link #acquire(long, TimeUnit)}
   */
  @Deprecated
  public Indigo acquireIndigo(long timeout, TimeUnit unit) throws InterruptedException {
    return acquire(timeout, unit);
  }

  /**
   * Releases an Indigo object back to the pool.
   *
   * @param indigo The Indigo object to be released.
   * @throws ObjectPoolException If the same object is released twice or a non-existent object is released.
   * @deprecated use {@link #release(Indigo)}
   */
  @Deprecated
  public void releaseIndigo(Indigo indigo) {
    release(indigo);
  }
}
