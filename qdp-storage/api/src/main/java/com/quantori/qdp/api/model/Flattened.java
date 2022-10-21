package com.quantori.qdp.api.model;

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
    private byte[] structure;
    private Map<String, String> properties;
    private String libraryId;
    private String libraryName;
    private String storageType;
  }

  /**
   * Reaction.
   */
  @Data
  @EqualsAndHashCode
  class Reaction {
    private String id;
    private String smiles;
    private String paragraphText;
    private String amount;
    private String source;
    private String reactionDocumentId;
    private String libraryId;
    private String storageType;
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
