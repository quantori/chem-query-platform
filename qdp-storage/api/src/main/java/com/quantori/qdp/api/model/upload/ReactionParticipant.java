package com.quantori.qdp.api.model.upload;

import com.quantori.qdp.api.model.ReactionParticipantRole;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * Reaction molecule extends {@link Molecule} with role and type.
 */
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class ReactionParticipant extends BasicMolecule {
  private String name;
  private String inchi;
  private String reactionId;
  private ReactionParticipantRole role = ReactionParticipantRole.none;
  private String type;
}
