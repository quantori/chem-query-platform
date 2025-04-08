package com.quantori.cqp.api.model.core;

import com.quantori.cqp.api.model.Criteria;
import com.quantori.cqp.api.model.ExactParams;
import com.quantori.cqp.api.model.ReactionParticipantRole;
import com.quantori.cqp.api.model.SearchProperty;
import com.quantori.cqp.api.model.SimilarityParams;
import com.quantori.cqp.api.model.SortParams;
import com.quantori.cqp.api.model.SubstructureParams;
import lombok.Builder;
import lombok.Getter;

import java.util.List;
import java.util.Map;

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
  private SortParams sortParams;
  private Criteria criteria;
}
