package com.quantori.qdp.core.source.model.molecule.search;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class RequestStructure {
  private final String storageName;
  private final List<String> indexNames;
  private final Request storageRequest;
  private final Predicate<StorageResultItem> resultFilter;
  private final Function<StorageResultItem, SearchResultItem> resultTransformer;
  private final Map<String, String> propertyTypes;
}
