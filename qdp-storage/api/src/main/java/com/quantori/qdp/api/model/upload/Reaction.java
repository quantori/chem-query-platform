package com.quantori.qdp.api.model.upload;

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
  private String id;
  private String reactionSmiles;
  private String reactionDocumentId;
  private String source;
  private String paragraphText;
  private String amount;
  private byte[] sub;
}
