package com.quantori.qdp.core.source.model.molecule.search;

import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class MultiStorageSearchRequest {
  private final ProcessingSettings processingSettings;
  private final Map<String, RequestStructure> requestStorageMap;

  public List<SearchRequest> toSearchRequests() {
    return requestStorageMap.values().stream()
        .map(requestStructure -> SearchRequest.builder()
            .requestStructure(requestStructure)
            .processingSettings(processingSettings)
            .build())
        .toList();
  }
}
