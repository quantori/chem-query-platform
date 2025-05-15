package com.quantori.cqp.api;

import com.quantori.cqp.api.model.ExactParams;
import com.quantori.cqp.api.model.Flattened;
import com.quantori.cqp.api.model.ReactionParticipantRole;
import com.quantori.cqp.api.model.StorageRequest;
import com.quantori.cqp.api.model.SubstructureParams;
import com.quantori.cqp.api.model.upload.ReactionUploadDocument;

import java.util.List;

/**
 * A storage must implement this interface to search and export reactions.
 * <p>
 * A reaction is always associated with a single library. Therefore, each search request accepts library identifiers in
 * which the search must be performed.
 */
public interface StorageReactions extends DataStorage<ReactionUploadDocument, Flattened.Reaction> {

  /**
   * Provides a fingerprint calculator for exact and substructure fingerprints.
   *
   * @return a fingerprint calculator instance
   */
  ReactionsFingerprintCalculator fingerPrintCalculator();

  /**
   * Perform reactions search by a storage request
   *
   * @param storageRequest a storage request
   * @return an iterator of found reactions
   */
  default List<SearchIterator<Flattened.Reaction>> searchIterator(StorageRequest storageRequest) {
    String searchQuery = storageRequest.getExactParams().getSearchQuery();
    ReactionParticipantRole role = storageRequest.getRole();
    ReactionsFingerprintCalculator fingerprintCalculator = fingerPrintCalculator();
    return switch (storageRequest.getSearchType()) {
      case exact -> {
        byte[] exactFingerprint = fingerprintCalculator.exactFingerprint(searchQuery);
        yield storageRequest.getIndexIds().stream()
            .map(libraryId -> searchExact(new String[] {libraryId}, exactFingerprint,
                storageRequest.getExactParams(), role))
            .toList();
      }

      case substructure -> switch (role) {
        case reaction -> {
          byte[] substructureFingerprint = fingerprintCalculator.substructureReactionFingerprint(searchQuery);
          yield storageRequest.getIndexIds().stream()
              .map(libraryId -> searchReactions(new String[] {libraryId}, substructureFingerprint,
                  storageRequest.getSubstructureParams()))
              .toList();
        }
        case reactant, product, spectator, solvent, catalyst -> {
          byte[] substructureFingerprint = fingerprintCalculator.substructureFingerprint(searchQuery);
          yield storageRequest.getIndexIds().stream()
              .map(libraryId -> searchSub(new String[] {libraryId}, substructureFingerprint,
                  storageRequest.getSubstructureParams(), role))
              .toList();
        }
        default -> throw new UnsupportedOperationException(String.format("Role %s is not supported", role));
      };

      case all -> {
        // substructure search with an empty search query
        byte[] substructureFingerprint = new byte[0];
        yield storageRequest.getIndexIds().stream()
          .map(libraryId -> searchReactions(new String[] {libraryId}, substructureFingerprint,
            SubstructureParams.builder().searchQuery("").build()))
          .toList();
      }

      default -> throw new UnsupportedOperationException(
          String.format("Search type %s is not supported", storageRequest.getSearchType()));
    };
  }

  /**
   * Get list of reactions in a specific library by id.
   *
   * @param libraryId  a library identifier
   * @param reactionId an array of reaction identifiers
   * @return a list of reactions
   */
  List<Flattened.Reaction> findById(String libraryId, String... reactionId);

  /**
   * Get list of reactions participants in a specific library by id.
   *
   * @param libraryId   a library identifier
   * @param reactionIds an array of reaction identifiers
   * @return a list of reactions participants
   */
  List<Flattened.ReactionParticipant> searchParticipantsByReactionId(String libraryId, String... reactionIds);

  /**
   * Perform the substructure reactions search by a reaction fingerprint.
   *
   * @param libraryIds                      an array of library identifiers in which the search must be performed
   * @param reactionSubstructureFingerprint a reaction fingerprint
   * @param substructureParams              parameters of the search {@link SubstructureParams}
   * @return an iterator of found reactions
   */
  SearchIterator<Flattened.Reaction> searchReactions(
      String[] libraryIds, byte[] reactionSubstructureFingerprint, SubstructureParams substructureParams);

  /**
   * Perform the exact reactions search by a reaction participant fingerprint.
   *
   * @param libraryIds                  an array of library identifiers in which the search must be performed
   * @param participantExactFingerprint a reaction participant fingerprint
   * @param exactParams                 parameters of the search {@link ExactParams}
   * @param role                        role of a reaction participant {@link ReactionParticipantRole}
   * @return an iterator of found reactions
   */
  SearchIterator<Flattened.Reaction> searchExact(
      String[] libraryIds, byte[] participantExactFingerprint, ExactParams exactParams, ReactionParticipantRole role);

  /**
   * Perform the substructure reactions search by a reaction participant fingerprint.
   *
   * @param libraryIds                         an array of library identifiers in which the search must be performed
   * @param participantSubstructureFingerprint a reaction participant fingerprint
   * @param substructureParams                 parameters of the search {@link SubstructureParams}
   * @param role                               role of a reaction participant {@link ReactionParticipantRole}
   * @return an iterator of found reactions
   */
  SearchIterator<Flattened.Reaction> searchSub(
      String[] libraryIds, byte[] participantSubstructureFingerprint, SubstructureParams substructureParams,
      ReactionParticipantRole role);

  /**
   * Perform the listing of all reactions in specified libraries
   *
   * @param libraryIds an array of library identifiers in which the search must be performed
   * @return an iterator of found reactions
   */
  default SearchIterator<Flattened.Reaction> searchAll(String[] libraryIds) {
    return searchReactions(libraryIds, new byte[0], SubstructureParams.builder().searchQuery("").build());
  }

  /**
   * Count reactions in a library.
   *
   * @param libraryId a library identifier
   * @return count of reactions in a library
   */
  long countElements(String libraryId);

  /**
   * Delete particular reactions from a library.
   *
   * @param libraryId a library identifier
   * @param reactionIds reaction identifiers to delete
   * @return true if reactions were deleted, false otherwise
   */
  boolean deleteReactions(String libraryId, List<String> reactionIds);
}
