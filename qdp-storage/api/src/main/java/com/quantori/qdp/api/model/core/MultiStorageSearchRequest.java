package com.quantori.qdp.api.model.core;

import java.util.Map;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class MultiStorageSearchRequest<S> {
  private final ProcessingSettings processingSettings;
  private final Map<String, RequestStructure<S>> requestStorageMap;
}
