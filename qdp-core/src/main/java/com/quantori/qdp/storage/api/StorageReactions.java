package com.quantori.qdp.storage.api;

import java.util.List;
import java.util.Optional;

public interface StorageReactions {
  Optional<Flattened.Reaction> searchById(Library library, String reactionId);

  List<Flattened.ReactionParticipant> searchParticipantsByReactionId(Library library, String reactionId);

  SearchIterator<Flattened.Reaction> searchReactions(
      String[] libraryIds, byte[] reactionSubstructureFingerprint);

  ReactionsWriter buildReactionWriter(Library library);

  long countElements(String libraryId);
}
