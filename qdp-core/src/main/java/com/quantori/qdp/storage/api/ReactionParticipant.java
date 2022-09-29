package com.quantori.qdp.storage.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Reaction molecule extends {@link Molecule} with role and type.
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ReactionParticipant extends Molecule {
  @JsonProperty("reactionId")
  private String reactionId;
  @JsonProperty("role")
  private ReactionParticipantRole reactionParticipantRole = ReactionParticipantRole.none;
  @JsonProperty("type")
  private String type;
}
