package com.quantori.qdp.api.model.upload;

import java.util.List;

/**
 * Reaction document with all data necessary to send to storage. It contains generic data and participants.
 *
 * @see Reaction
 * @see ReactionParticipant
 */
public record ReactionUploadDocument(Reaction reaction, List<ReactionParticipant> participantEntities) {
}
