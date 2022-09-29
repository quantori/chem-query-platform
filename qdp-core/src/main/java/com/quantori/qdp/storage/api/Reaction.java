package com.quantori.qdp.storage.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Reaction document with reaction substructure fingerprint.
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode
public class Reaction {
  @JsonProperty("structure")
  private String reactionSmiles;
  @JsonProperty("reactionId")
  private String documentId;
  @JsonProperty("source")
  private String source;
  @JsonProperty("description")
  private String paragraphText;
  @JsonProperty("amount")
  private String amount;
  @JsonProperty("sub")
  private byte[] sub;
}
