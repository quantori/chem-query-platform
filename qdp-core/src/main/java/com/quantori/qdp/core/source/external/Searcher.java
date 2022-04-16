package com.quantori.qdp.core.source.external;

import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import com.quantori.qdp.core.source.model.molecule.search.SearchResult;

interface Searcher extends AutoCloseable {
  SearchResult searchNext(int limit);

  SearchResult searchStat();

  void close();

  SearchRequest getSearchRequest();
}
