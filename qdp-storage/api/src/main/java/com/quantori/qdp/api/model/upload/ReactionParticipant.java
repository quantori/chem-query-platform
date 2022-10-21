package com.quantori.qdp.api.model.upload;

import com.quantori.qdp.api.model.ReactionParticipantRole;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Reaction molecule extends {@link Molecule} with role and type.
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ReactionParticipant extends BasicMolecule {
  private String name;
  private String smiles;
  private String inchi;
  private String reactionId;
  private ReactionParticipantRole role = ReactionParticipantRole.none;
  private String type;
}
