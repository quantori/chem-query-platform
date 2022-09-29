package com.quantori.qdp.storage.solr;

import com.quantori.qdp.storage.api.FingerPrintUtilities;
import com.quantori.qdp.storage.api.Reaction;
import lombok.Getter;
import org.apache.solr.client.solrj.beans.Field;

@Getter
class SolrReactionDocument {
  @Field("reactionSmiles")
  private final String structure;
  @Field("sub")
  private final String subHash;
  @Field("reactionId")
  private final String documentId;
  @Field("source")
  private final String source;
  @Field("description")
  private final String description;
  @Field("amount")
  private final String amount;
  @Field("id")
  private String id;

  SolrReactionDocument(Reaction reaction) {
    this.amount = reaction.getAmount();
    this.description = reaction.getParagraphText();
    this.documentId = reaction.getDocumentId();
    this.source = reaction.getSource();
    this.subHash = FingerPrintUtilities.substructureHash(reaction.getSub());
    this.structure = reaction.getReactionSmiles();
  }

  SolrReactionDocument(Reaction reaction, String id) {
    this(reaction);
    this.id = id;
  }
}
