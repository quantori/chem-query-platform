package com.quantori.qdp.core.source;

import com.quantori.qdp.api.model.core.SearchItem;
import com.quantori.qdp.api.model.core.SearchResult;
import java.util.concurrent.CompletionStage;

public interface Searcher<S extends SearchItem> extends AutoCloseable {
  CompletionStage<SearchResult<S>> searchNext(int limit);

  void close();

  String getUser();
}
