package com.quantori.qdp.core.source.model;

/**
 * PAGE_BY_PAGE - strategy which fetches data synchronously with user request but with buffering
 * PAGE_FROM_STREAM - strategy which fetches data asynchronously with user request. It works in background after user request is completed.
 */
public enum SearchStrategy {
  PAGE_BY_PAGE,
  PAGE_FROM_STREAM
}
