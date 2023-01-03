package com.quantori.qdp.api.model.core;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class RequestStructure<S> {
  private final String storageName;
  private final List<String> indexNames;
  private final StorageRequest storageRequest;
  private final Predicate<StorageItem> resultFilter;
  private final Function<StorageItem, S> resultTransformer;
  private final Map<String, String> propertyTypes;
}
