package com.quantori.qdp.storage.solr;

import com.quantori.qdp.storage.api.ReactionParticipant;
import lombok.Getter;
import org.apache.solr.client.solrj.beans.Field;

@Getter
class SolrReactionParticipantDocument extends SolrMoleculeDocument {

  @Field("reactionId")
  private final String reactionId;
  @Field("role")
  private final String role;
  @Field("type")
  private final String type;

  SolrReactionParticipantDocument(final ReactionParticipant reactionParticipant, final String libraryId) {
    super(reactionParticipant, libraryId);
    this.reactionId = reactionParticipant.getReactionId();
    this.role = reactionParticipant.getReactionParticipantRole().toString();
    this.type = reactionParticipant.getType();
  }
}
