package com.quantori.qdp.storage.api;

import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * Flattened search result structure.
 */
public interface Flattened {

  /**
   * Molecule/Compound.
   */
  @Data
  @EqualsAndHashCode
  class Molecule {
    private String id;
    private String decodedStructure;
    private String encodedStructure;
    private String libraryId;
    private String libraryName;
    private Map<String, String> properties;
    private String reactionId;
    private ReactionParticipantRole role;
    private StorageType storageType;
  }

  /**
   * Reaction.
   */
  @Data
  @EqualsAndHashCode
  class Reaction {
    private String id;
    private String structure;
    private String description;
    private String amount;
    private String source;
    private String reactionId;
    private String libraryId;
    private StorageType storageType;
  }

  @Data
  @EqualsAndHashCode
  class ReactionParticipant {
    private String id;
    private String name;
    private String smiles;
    private String inchi;
    private ReactionParticipantRole role;
    private String type;
  }
}
