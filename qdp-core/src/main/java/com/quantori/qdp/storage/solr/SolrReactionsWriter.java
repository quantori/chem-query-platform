package com.quantori.qdp.storage.solr;

import com.quantori.qdp.storage.api.Library;
import com.quantori.qdp.storage.api.ReactionParticipant;
import com.quantori.qdp.storage.api.ReactionUploadDocument;
import com.quantori.qdp.storage.api.ReactionsWriter;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;

@Slf4j
public class SolrReactionsWriter implements ReactionsWriter {
  private static final int COMMIT_WITHIN_MS = 1000;
  private final SolrClient client;
  private final Consumer<Library> onCloseCallback;
  private final boolean rollbackEnabled;
  private final String reactionCollection;
  private final String reactionParticipantsCollection;
  private final Library library;

  SolrReactionsWriter(SolrClient solrClient, Library library, Consumer<Library> onCloseCallback,
                      boolean rollbackEnabled) {
    this.client = solrClient;
    this.library = library;
    this.onCloseCallback = onCloseCallback;
    this.rollbackEnabled = rollbackEnabled;
    this.reactionCollection = SolrStorageReactions.REACTIONS_STORE_PREFIX + library.getId();
    this.reactionParticipantsCollection = SolrStorageReactions.REACTION_PARTICIPANTS_STORE_PREFIX + library.getId();
  }

  @Override
  public void write(ReactionUploadDocument uploadDocument) {
    var uuid = UUID.randomUUID().toString();
    var solrReactionDocument = new SolrReactionDocument(uploadDocument.reaction(), uuid);
    try {
      var updateResponse = client.addBean(reactionCollection, solrReactionDocument, COMMIT_WITHIN_MS);
      if (updateResponse.getStatus() == 0) {
        var hasFailure = uploadParticipants(uploadDocument.participantEntities(), uuid);
        if (hasFailure) {
          throw new RuntimeException(String.format(
              "Not all participants uploaded successfully in SOLR storage to library [%s] for document [%s]",
              library, solrReactionDocument));
        } else {
          client.commit(reactionCollection);
          client.commit(reactionParticipantsCollection);
        }
      } else {
        log.debug("Reaction: {} upload failed with response status: {}", solrReactionDocument,
            updateResponse.getStatus());
      }
    } catch (Exception e) {
      log.warn("Failed to add entry [{}] to library {}. Trying to rollback changes", uploadDocument, library, e);
      rollbackCommits();
    }
  }

  @Override
  public void flush() {
    try {
      client.commit(reactionCollection);
      client.commit(reactionParticipantsCollection);
    } catch (SolrServerException | IOException e) {
      log.error("Failed to flush and commit library {}", library, e);
      rollbackCommits();
    }
  }

  @Override
  public void close() {
    try {
      flush();
    } finally {
      try {
        onCloseCallback.accept(library);
      } finally {
        flush();
      }
    }
  }

  private void rollbackCommits() {
    if (rollbackEnabled) {
      try {
        client.rollback(reactionCollection);
      } catch (SolrServerException | IOException e) {
        log.error("Error while rollback", e);
      } finally {
        try {
          client.rollback(reactionParticipantsCollection);
        } catch (SolrServerException | IOException solrServerException) {
          log.error("Error while rollback", solrServerException);
        }
      }
    }
  }

  private boolean uploadParticipants(final List<ReactionParticipant> participants, final String reactionId) {
    boolean hasFalure = false;
    for (ReactionParticipant participant : participants) {
      var success = addParticipant(participant, reactionId);
      if (!success) {
        hasFalure = true;
      }
    }
    return hasFalure;
  }

  private boolean addParticipant(ReactionParticipant reactionParticipant, String reactionId) {
    reactionParticipant.setReactionId(reactionId);
    reactionParticipant.setId(UUID.randomUUID().toString());
    var obj = new SolrReactionParticipantDocument(reactionParticipant, library.getId());
    try {
      var updateResponse = client.addBean(reactionParticipantsCollection, obj, COMMIT_WITHIN_MS);
      if (updateResponse.getStatus() != 0) {
        throw new SolrStorageException(String.format("Couldn't upload participant: %s, SOLR error response: %s",
            reactionParticipant, updateResponse.getStatus()));
      }
      return true;
    } catch (Exception e) {
      log.error("Failed to add reaction reactionParticipant [{}] to solr storage", reactionParticipant, e);
      return false;
    }
  }
}
