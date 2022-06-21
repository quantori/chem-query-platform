package com.quantori.qdp.core.source.model;

import com.quantori.qdp.core.source.model.molecule.search.StorageResultItem;
import java.util.List;

public interface DataSearcher extends AutoCloseable {
  List<? extends StorageResultItem> next();
}
