package com.quantori.cqp.api.indigo;

import lombok.SneakyThrows;

import java.util.concurrent.TimeUnit;

public class ObjectProvider<E> {

  /**
   * Pool of objects
   */
  private final ObjectPool<E> objectPool;

  /**
   * How long to wait to take an object before giving up in units of seconds
   */
  private final int timeout;

  public ObjectProvider(ObjectPool<E> objectPool, int timeoutInSeconds) {

    this.objectPool = objectPool;
    this.timeout = timeoutInSeconds;
  }

  /**
   * Acquires an object from the pool. Blocks and waits until an object is available.
   *
   * @return An object from the pool.
   */
  @SneakyThrows(InterruptedException.class)
  public E take() {

    return objectPool.acquire(timeout, TimeUnit.SECONDS);
  }

  /**
   * Releases an object back to the pool.
   *
   * @param element The object to be released.
   */
  public void offer(E element) {

    objectPool.release(element);
  }
}
