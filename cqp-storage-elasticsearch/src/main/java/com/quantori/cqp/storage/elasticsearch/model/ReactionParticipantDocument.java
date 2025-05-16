package com.quantori.cqp.storage.elasticsearch.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.quantori.cqp.api.model.ReactionParticipantRole;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ReactionParticipantDocument extends BasicMoleculeDocument {

  @JsonProperty("name")
  private String name;

  @JsonProperty("inchi")
  private String inchi;

  @JsonProperty("reactionId")
  private String reactionId;

  @JsonProperty("role")
  private ReactionParticipantRole reactionParticipantRole = ReactionParticipantRole.none;

  @JsonProperty("type")
  private String type;
}
