package com.quantori.qdp.api.model;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.quantori.qdp.api.model.core.SearchItem;
import com.quantori.qdp.api.model.core.StorageItem;
import java.time.Instant;
import java.util.Map;
import lombok.Data;

/**
 * Flattened search result structure.
 */
public interface Flattened {

  /**
   * Molecule/Compound.
   */
  @Data
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, defaultImpl = Molecule.class)
  class Molecule implements SearchItem, StorageItem {
    private String id;
    private String smiles;
    private byte[] structure;
    private Map<String, String> properties;
    private String libraryId;
    private String libraryName;
    private String storageType;
    private Long customOrder;
    private Instant createdStamp;
    private Instant updatedStamp;
  }

  /**
   * Reaction.
   */
  @Data
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, defaultImpl = Reaction.class)
  class Reaction implements SearchItem, StorageItem {
    private String id;
    private String smiles;
    private String paragraphText;
    private String amount;
    private String source;
    private String reactionDocumentId;
    private String libraryId;
    private String storageType;
    private Instant createdStamp;
    private Instant updatedStamp;
  }

  @Data
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, defaultImpl = ReactionParticipant.class)
  class ReactionParticipant {
    private String id;
    private String name;
    private String smiles;
    private String inchi;
    private ReactionParticipantRole role;
    private String type;
  }
}
