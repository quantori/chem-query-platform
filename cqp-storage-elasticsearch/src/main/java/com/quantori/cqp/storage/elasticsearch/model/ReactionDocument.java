package com.quantori.cqp.storage.elasticsearch.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ReactionDocument {

  @JsonProperty("reactionSmiles")
  private String reactionSmiles;

  @JsonProperty("reactionDocumentId")
  private String reactionDocumentId;

  @JsonProperty("source")
  private String source;

  @JsonProperty("paragraphText")
  private String paragraphText;

  @JsonProperty("amount")
  private String amount;

  @JsonProperty("sub")
  private String subHash;

  @JsonProperty("created_timestamp")
  private Long createdStamp;

  @JsonProperty("updated_timestamp")
  private Long updatedStamp;
}
