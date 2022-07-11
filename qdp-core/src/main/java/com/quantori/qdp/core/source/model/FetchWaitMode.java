package com.quantori.qdp.core.source.model;

/**
 * NO_WAIT - result of search can have fewer data then requested. Returns data from the buffer without waiting. User have to request again to fetch more data.
 * Works only in combination with  PAGE_FROM_STREAM strategy.
 * <p>
 * WAIT_COMPLETE - user will wait until all data will be fetched according to provided limit parameter. It will return fewer data then requested only when search is completed.
 */
public enum FetchWaitMode {
  NO_WAIT,
  WAIT_COMPLETE
}
