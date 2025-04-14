package com.quantori.cqp.api.model.core;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder(toBuilder = true)
public class SearchResult<S extends SearchItem> {
  private final String searchId;

  /**
   * Indicates the search is finished and no more results available.
   */
  private final boolean searchFinished;

  /**
   * How many items returned by storage (before filtering by {@link SearchRequest#getResultFilter()}
   */
  private final long foundCount;
  /**
   * How many items matched by {@link SearchRequest#getResultFilter()} and transformed by
   * {@link SearchRequest#getResultTransformer()}
   */
  private final long matchedByFilterCount;
  /**
   * List of errors in transformation step, filter step and (if happens) in data searcher
   */
  private final List<SearchError> errors;
  /**
   * Counter results found by count task
   */
  private final long resultCount;

  private final List<S> results;
}
