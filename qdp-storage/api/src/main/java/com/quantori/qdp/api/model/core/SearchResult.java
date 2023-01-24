package com.quantori.qdp.api.model.core;

import java.util.List;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder(toBuilder = true)
public class SearchResult<S extends SearchItem> {
  private final String searchId;

  /**
   * Indicates the search is finished and no more results available.
   */
  private final boolean searchFinished;
  /**
   * Indicated the count task finished.
   */
  private final boolean countFinished;

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
   * Item count found by separated count task which counts items only
   */
  private final long resultCount;

  private final List<S> results;
}
