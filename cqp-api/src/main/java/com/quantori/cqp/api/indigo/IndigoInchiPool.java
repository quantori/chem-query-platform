package com.quantori.cqp.api.indigo;

/**
 * Pool of IndigoInchi objects.
 */
public class IndigoInchiPool extends ObjectPool<IndigoInchi> {

  /**
   * Creates a new IndigoInchi pool.
   * Initializes the IndigoInchi pool on application startup.
   */
  IndigoInchiPool(int maximumPoolSize) {
    super(maximumPoolSize, IndigoFactory::createNewIndigoInchi, IndigoInchi::getSid);
  }
}
