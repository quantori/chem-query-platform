package com.quantori.qdp.storage.solr;

public class InitFailedException extends Exception {
  public InitFailedException(final String collectionName, final Exception e) {
    super("Failed to initialize collection " + collectionName, e);
  }
}
