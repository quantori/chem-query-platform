package com.quantori.qdp.core.source.model;

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
   * How many items returned by storage (before filtering by {@link RequestStructure#getResultFilter()}
   */
  private final long foundCount;
  /**
   * How many items matched by {@link RequestStructure#getResultFilter()} and transformed by
   * {@link RequestStructure#getResultTransformer()}
   */
  private final long matchedByFilterCount;
  /**
   * Count of errors in transformation and filter steps
   */
  private final long errorCount;
  /**
   * Item count found by separated count task which counts items only
   */
  private final long resultCount;

  private final List<S> results;
}
