package com.quantori.qdp.api.model.core;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class RequestStructure<S extends SearchItem, I extends StorageItem> {
  private final String storageName;
  private final List<String> indexNames;
  private final StorageRequest storageRequest;
  private final Predicate<I> resultFilter;
  private final Function<I, S> resultTransformer;
  private final Map<String, String> propertyTypes;
}
