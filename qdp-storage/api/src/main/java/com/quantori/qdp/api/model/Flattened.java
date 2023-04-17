package com.quantori.qdp.api.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.quantori.qdp.api.model.core.SearchItem;
import com.quantori.qdp.api.model.core.StorageItem;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Flattened search result structure.
 */
public interface Flattened {

  /**
   * Molecule/Compound.
   */
  @Data
  @ToString(callSuper = true)
  @EqualsAndHashCode(callSuper = true)
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, defaultImpl = Molecule.class)
  class Molecule extends com.quantori.qdp.api.model.upload.Molecule implements SearchItem, StorageItem {
    private String libraryId;
    private String libraryName;
    private String storageType;
  }

  /**
   * Reaction.
   */
  @Data
  @ToString(callSuper = true)
  @EqualsAndHashCode(callSuper = true)
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, defaultImpl = Reaction.class)
  class Reaction extends com.quantori.qdp.api.model.upload.Reaction implements SearchItem, StorageItem {
    private String libraryId;
    private String libraryName;
    private String storageType;
  }

  @Data
  @ToString(callSuper = true)
  @EqualsAndHashCode(callSuper = true)
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, defaultImpl = ReactionParticipant.class)
  class ReactionParticipant extends com.quantori.qdp.api.model.upload.ReactionParticipant
      implements SearchItem, StorageItem {
  }
}
