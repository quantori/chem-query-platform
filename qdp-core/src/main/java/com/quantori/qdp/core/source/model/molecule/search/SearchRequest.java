package com.quantori.qdp.core.source.model.molecule.search;

import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class SearchRequest {
  private final ProcessingSettings processingSettings;
  private final RequestStructure requestStructure;
}
