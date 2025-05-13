package com.quantori.cqp.api.indigo;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Pool of objects.
 */
public class ObjectPool<E> {

  public static final boolean IN_POOL = true;
  public static final boolean OUT_OF_POOL = false;
  protected final BlockingQueue<E> pool;
  protected final ConcurrentHashMap<Long, Boolean> availabilityMap;
  protected final Function<E, Long> idFunction;

  /**
   * Creates a new pool.
   * Initializes the pool on application startup.
   */
  ObjectPool(int maximumPoolSize, Supplier<E> objectSupplier, Function<E, Long> idFunction) {

    this.pool = new LinkedBlockingQueue<>(maximumPoolSize);
    this.availabilityMap = new ConcurrentHashMap<>();
    this.idFunction = idFunction;

    for (int i = 0; i < maximumPoolSize; i++) {
      E element = objectSupplier.get();
      pool.offer(element);
      availabilityMap.put(idFunction.apply(element), IN_POOL);
    }
  }

  /**
   * Acquires an object from the pool. Blocks and waits until an object is available for the provided time.
   * If the poll method returns a null value or the replace method returns false, it throws an ObjectPoolException.
   *
   * @param timeout how long to wait before giving up, in units of {@code unit}
   * @param unit a {@code TimeUnit} determining how to interpret the {@code timeout} parameter
   *
   * @return An object from the pool.
   *
   * @throws InterruptedException if interrupted while waiting to acquire an object.
   * @throws ObjectPoolException if the pool and the availability map are out of sync.
   */
  public E acquire(long timeout, TimeUnit unit) throws InterruptedException {

    E element = pool.poll(timeout, unit);
    if (Objects.isNull(element)) {
      throw new ObjectPoolException("Timeout: No object available within the specified time.");
    }
    boolean replaced = availabilityMap.replace(idFunction.apply(element), IN_POOL, OUT_OF_POOL);
    if (!replaced) {
      throw new ObjectPoolException("Pool and availability map are out of sync.");
    }
    return element;
  }

  /**
   * Releases an object back to the pool.
   *
   * @param element The object to be released.
   * @throws ObjectPoolException If the same object is released twice or a non-existent object is released.
   */
  public void release(E element) {

    if (availabilityMap.replace(idFunction.apply(element), OUT_OF_POOL, IN_POOL)) {
      pool.add(element);
    } else {
      throw new ObjectPoolException("Cannot release the same object twice or non-existent object.");
    }
  }
}
