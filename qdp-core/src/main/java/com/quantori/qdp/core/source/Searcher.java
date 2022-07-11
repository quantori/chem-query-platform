package com.quantori.qdp.core.source;

import com.quantori.qdp.core.source.model.SearchResult;
import java.util.concurrent.CompletionStage;

public interface Searcher extends AutoCloseable {
  CompletionStage<SearchResult> searchNext(int limit);

  void close();

  String getUser();
}
