package com.quantori.qdp.core.source.model.molecule;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.quantori.qdp.core.source.model.reaction.ReactionParticipantRole;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Reaction molecule extends {@link Molecule} with role and type.
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ReactionMolecule extends Molecule {
  @JsonProperty("reactionId")
  private String reactionId;
  @JsonProperty("role")
  private ReactionParticipantRole reactionParticipantRole = ReactionParticipantRole.none;
  @JsonProperty("type")
  private String type;
  private MoleculeFingerprint genericMolecule;


}

