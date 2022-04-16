package com.quantori.qdp.core.source.model;

import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import java.util.List;

public interface DataSearcher extends AutoCloseable {
  List<? extends SearchRequest.StorageResultItem> next();
}
