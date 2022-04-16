package com.quantori.qdp.core.source.model.molecule.search;

import java.util.ArrayList;
import java.util.List;

public class SearchResult {
  private final String searchId;

  /** Indicates the search is finished and no more results available. */
  private final boolean searchFinished;
  /** Indicated the count task finished. */
  private final boolean countFinished;

  /** How many items returned by storage (before filtering by {@link SearchRequest#getResultFilter()} */
  private final long foundByStorageCount;
  /** How many items matched by {@link SearchRequest#getResultFilter()} and transformed by
   * {@link SearchRequest#getResultTransformer()} */
  private final long matchedByFilterCount;
  /** Count of errors in transformation and filter steps */
  private final long errorCount;
  /** Item count found by separated count task which counts items only */
  private final long resultCount;

  private final List<SearchResultItem> results;

  private SearchResult(String searchId, boolean searchFinished, boolean countFinished, long foundByStorageCount,
                       long matchedByFilterCount, long errorCount, long resultCount,
                       List<SearchResultItem> results) {
    this.searchId = searchId;
    this.searchFinished = searchFinished;
    this.countFinished = countFinished;
    this.foundByStorageCount = foundByStorageCount;
    this.matchedByFilterCount = matchedByFilterCount;
    this.errorCount = errorCount;
    this.resultCount = resultCount;
    this.results = results;
  }

  public boolean isSearchFinished() {
    return searchFinished;
  }

  public boolean isCountFinished() {
    return countFinished;
  }

  public String getSearchId() {
    return searchId;
  }

  public long getFoundByStorageCount() {
    return foundByStorageCount;
  }

  public long getMatchedByFilterCount() {
    return matchedByFilterCount;
  }

  public long getErrorCount() {
    return errorCount;
  }

  public long getResultCount() {
    return resultCount;
  }

  public List<? extends SearchResultItem> getResults() {
    return results;
  }

  public Builder copyBuilder() {
    return new Builder(this);
  }

  public static  Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String searchId;
    private boolean searchFinished;
    private boolean countFinished;

    private long foundByStorageCount;
    private long matchedByFilterCount;
    private long errorCount;
    private long resultCount;

    private List<SearchResultItem> results;

    public Builder() {
    }

    public Builder(SearchResult result) {
      this.searchId = result.searchId;
      this.searchFinished = result.searchFinished;
      this.countFinished = result.countFinished;
      this.foundByStorageCount = result.foundByStorageCount;
      this.matchedByFilterCount = result.matchedByFilterCount;
      this.errorCount = result.errorCount;
      this.resultCount = result.resultCount;
      this.results = result.results != null ? new ArrayList<>(result.results) : null;
    }

    public Builder searchId(String searchId) {
      this.searchId = searchId;
      return this;
    }

    public Builder searchFinished(boolean searchFinished) {
      this.searchFinished = searchFinished;
      return this;
    }

    public Builder countFinished(boolean countFinished) {
      this.countFinished = countFinished;
      return this;
    }

    public Builder foundByStorageCount(long foundByStorageCount) {
      this.foundByStorageCount = foundByStorageCount;
      return this;
    }

    public Builder matchedByFilterCount(long matchedByFilterCount) {
      this.matchedByFilterCount = matchedByFilterCount;
      return this;
    }

    public Builder errorCount(long errorCount) {
      this.errorCount = errorCount;
      return this;
    }

    public Builder resultCount(long resultCount) {
      this.resultCount = resultCount;
      return this;
    }

    public Builder results(List<SearchResultItem> results) {
      this.results = results;
      return this;
    }

    public SearchResult build() {
      return new SearchResult(
          searchId,
          searchFinished,
          countFinished,
          foundByStorageCount,
          matchedByFilterCount,
          errorCount,
          resultCount,
          results
      );
    }
  }
}
