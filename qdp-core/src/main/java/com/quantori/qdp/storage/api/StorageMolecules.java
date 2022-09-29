package com.quantori.qdp.storage.api;

import java.util.List;

public interface StorageMolecules {
  SearchIterator<Flattened.Molecule> searchSim(String[] libraryIds, byte[] moleculeSimilarityFingerprint,
                                               List<SearchProperty> properties, Similarity similarity);

  SearchIterator<Flattened.Molecule> searchSub(
      String[] libraryIds, byte[] moleculeSubstructureFingerprint, List<SearchProperty> properties,
      ReactionParticipantRole reactionParticipantRole);

  SearchIterator<Flattened.Molecule> searchExact(
      String[] libraryIds, byte[] moleculeExactFingerprint, List<SearchProperty> properties,
      ReactionParticipantRole reactionParticipantRole);

  List<Flattened.Molecule> findById(String[] libraryIds, String... moleculeIds);

  MoleculesWriter buildMoleculeWriter(Library library);

  long countElements(Library library);
}
