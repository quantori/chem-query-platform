package com.quantori.qdp.core.source.external;

import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import com.quantori.qdp.core.source.model.molecule.search.SearchResult;

import java.util.concurrent.CompletionStage;

public interface Searcher extends AutoCloseable {
  CompletionStage<SearchResult> searchNext(int limit);

  CompletionStage<SearchResult> searchStat();

  void close();

  SearchRequest getSearchRequest();
}
