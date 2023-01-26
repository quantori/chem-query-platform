package com.quantori.qdp.api.model.core;

import com.quantori.qdp.api.model.ExactParams;
import com.quantori.qdp.api.model.ReactionParticipantRole;
import com.quantori.qdp.api.model.SearchProperty;
import com.quantori.qdp.api.model.SimilarityParams;
import com.quantori.qdp.api.model.SubstructureParams;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class StorageRequest {
  private final String storageName;
  private final List<String> indexIds;
  private final SearchType searchType;
  private final Map<String, String> searchProperties;
  private ExactParams exactParams;
  private SubstructureParams substructureParams;
  private SimilarityParams similarityParams;
  private ReactionParticipantRole role;
  private byte[] queryFingerprint;
  private List<SearchProperty> properties;

  public enum SearchType {
    exact, substructure, similarity
  }

}
