package com.quantori.qdp.core.source.model;

import java.util.Map;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class MultiStorageSearchRequest {
  private final ProcessingSettings processingSettings;
  private final Map<String, RequestStructure> requestStorageMap;
}
