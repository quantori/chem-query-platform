package com.quantori.qdp.api.model.upload;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.quantori.qdp.api.model.core.StorageUploadItem;
import java.util.List;

/**
 * Reaction document with all data necessary to send to storage. It contains generic data and participants.
 *
 * @see Reaction
 * @see ReactionParticipant
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, defaultImpl = ReactionUploadDocument.class)
public record ReactionUploadDocument(Reaction reaction, List<ReactionParticipant> participantEntities)
    implements StorageUploadItem {
}
