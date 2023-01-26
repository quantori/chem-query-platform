package com.quantori.qdp.api.model.upload;

import lombok.Data;

/**
 * Reaction document with reaction substructure fingerprint.
 */
@Data
public class Reaction {
  private String id;
  private String reactionSmiles;
  private String reactionDocumentId;
  private String source;
  private String paragraphText;
  private String amount;
  private byte[] sub;
}
