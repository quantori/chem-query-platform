package com.quantori.qdp.core.source.model.reaction;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class AbstractReactionDocument {
  @JsonProperty("structure")
  protected String reactionSmiles;
  @JsonProperty("reactionId")
  protected String documentId;
  @JsonProperty("source")
  protected String source;
  @JsonProperty("description")
  protected String paragraphText;
  @JsonProperty("amount")
  protected String amount;
}