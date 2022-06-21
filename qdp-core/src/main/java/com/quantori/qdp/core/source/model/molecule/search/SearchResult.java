package com.quantori.qdp.core.source.model.molecule.search;

import java.util.List;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder(toBuilder = true)
public class SearchResult {
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
   * How many items returned by storage (before filtering by {@link SearchRequest#getRequestStructure()#getResultFilter()}
   */
  private final long foundByStorageCount;
  /**
   * How many items matched by {@link SearchRequest#getRequestStructure()#getResultFilter()} and transformed by
   * {@link SearchRequest#getRequestStructure()#getResultTransformer()}
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

  private final List<SearchResultItem> results;

  public List<? extends SearchResultItem> getResults() {
    return results;
  }
}
