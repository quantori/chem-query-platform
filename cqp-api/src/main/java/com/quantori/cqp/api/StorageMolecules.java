package com.quantori.cqp.api;

import com.quantori.cqp.api.model.ExactParams;
import com.quantori.cqp.api.model.Flattened;
import com.quantori.cqp.api.model.SearchProperty;
import com.quantori.cqp.api.model.SimilarityParams;
import com.quantori.cqp.api.model.SortParams;
import com.quantori.cqp.api.model.StorageRequest;
import com.quantori.cqp.api.model.SubstructureParams;
import com.quantori.cqp.api.model.upload.Molecule;

import java.util.List;

/**
 * A storage must implement this interface to search and export molecules.
 * <p>
 * A molecule is always associated with a single library. Therefore, each search request accepts library identifiers in
 * which the search must be performed.
 */
public interface StorageMolecules extends DataStorage<Molecule, Flattened.Molecule> {

  /**
   * Provides a fingerprint calculator for exact, substructure, and similarity fingerprints.
   *
   * @return a fingerprint calculator instance
   */
  MoleculesFingerprintCalculator fingerPrintCalculator();

  /**
   * Perform molecules search by a storage request
   *
   * @param storageRequest a storage request
   * @return an iterator of found molecules
   */
  default List<SearchIterator<Flattened.Molecule>> searchIterator(StorageRequest storageRequest) {
    MoleculesFingerprintCalculator fingerprintCalculator = fingerPrintCalculator();
    return switch (storageRequest.getSearchType()) {
      case exact -> {
        String searchQuery = storageRequest.getExactParams().getSearchQuery();
        byte[] exactFingerprint = fingerprintCalculator.exactFingerprint(searchQuery);
        List<SearchProperty> properties = storageRequest.getProperties();

        yield storageRequest.getIndexIds().stream()
          .map(libraryId -> searchExact(new String[] {libraryId}, exactFingerprint, properties,
            storageRequest.getExactParams(), storageRequest.getSortParams(), storageRequest.getCriteria()))
          .toList();
      }
      case substructure -> {
        String searchQuery = storageRequest.getSubstructureParams().getSearchQuery();
        byte[] substructureFingerprint = fingerprintCalculator.substructureFingerprint(searchQuery);
        List<SearchProperty> properties = storageRequest.getProperties();
        yield storageRequest.getIndexIds().stream()
          .map(libraryId -> searchSub(new String[] {libraryId}, substructureFingerprint, properties,
            storageRequest.getSubstructureParams(), storageRequest.getSortParams(), storageRequest.getCriteria()))
          .toList();
      }
      case similarity -> {
        String searchQuery = storageRequest.getSubstructureParams().getSearchQuery();
        byte[] similarityFingerprint = fingerprintCalculator.similarityFingerprint(searchQuery);
        List<SearchProperty> properties = storageRequest.getProperties();
        yield storageRequest.getIndexIds().stream()
          .map(libraryId -> searchSim(new String[] {libraryId}, similarityFingerprint, properties,
            storageRequest.getSimilarityParams(), storageRequest.getSortParams(), storageRequest.getCriteria()))
          .toList();
      }
      case all -> {
        // substructure search with an empty search query
        var searchQuery = "";
        byte[] substructureFingerprint = fingerprintCalculator.substructureFingerprint(searchQuery);
        List<SearchProperty> properties = storageRequest.getProperties();
        yield storageRequest.getIndexIds().stream()
          .map(libraryId -> searchSub(new String[] {libraryId}, substructureFingerprint, properties,
            SubstructureParams.builder().searchQuery(searchQuery).build(), storageRequest.getSortParams(),
            storageRequest.getCriteria()))
          .toList();
      }
    };
  }

  /**
   * Perform the exact molecules search by a fingerprint.
   *
   * @param libraryIds               an array of library identifiers in which the search must be performed
   * @param moleculeExactFingerprint a molecule fingerprint
   * @param properties               list of molecule properties that can be used by a storage implementation to reduce
   *                                 results
   * @param sortParams               list of sort options
   * @param criteria                 filter criteria
   * @return an iterator of found molecules
   */
  SearchIterator<Flattened.Molecule> searchExact(
      String[] libraryIds, byte[] moleculeExactFingerprint, List<SearchProperty> properties,
      ExactParams exactParams, SortParams sortParams, Criteria criteria);

  /**
   * Perform the substructure molecules search by a fingerprint.
   *
   * @param libraryIds         an array of library identifiers in which the search must be performed
   * @param queryFingerprint   a molecule fingerprint
   * @param properties         list of molecule properties that can be used by a storage implementation to reduce
   *                           results
   * @param substructureParams parameters of the search {@link SubstructureParams}
   * @param sortParams         list of sort options
   * @param criteria           filter criteria
   * @return an iterator of found molecules
   */
  SearchIterator<Flattened.Molecule> searchSub(
      String[] libraryIds, byte[] queryFingerprint, List<SearchProperty> properties,
      SubstructureParams substructureParams, SortParams sortParams, Criteria criteria);

  /**
   * Perform the similarity molecules search by a fingerprint.
   *
   * @param libraryIds                    an array of library identifiers in which the search must be performed
   * @param moleculeSimilarityFingerprint a molecule fingerprint
   * @param properties                    list of molecule properties that can be used by a storage implementation to
   *                                      reduce results
   * @param similarityParams              parameters of the search {@link SimilarityParams}
   * @param sortParams                    list of sort options
   * @param criteria                      filter criteria
   * @return an iterator of found molecules
   */
  SearchIterator<Flattened.Molecule> searchSim(
      String[] libraryIds, byte[] moleculeSimilarityFingerprint, List<SearchProperty> properties,
      SimilarityParams similarityParams, SortParams sortParams, Criteria criteria);

  /**
   * Perform the listing of all molecules in specified libraries
   *
   * @param libraryIds an array of library identifiers in which the search must be performed
   * @param properties list of molecule properties that can be used by a storage implementation to
   *                   reduce results
   * @param sortParams list of sort options
   * @param criteria   filter criteria
   * @return an iterator of found molecules
   */
  default SearchIterator<Flattened.Molecule> searchAll(String[] libraryIds, List<SearchProperty> properties,
                                                       SortParams sortParams, Criteria criteria) {
    var searchQuery = "";
    return searchSub(libraryIds, fingerPrintCalculator().substructureFingerprint(searchQuery), properties,
      SubstructureParams.builder().searchQuery(searchQuery).build(), sortParams, criteria);
  }

  /**
   * Get a list of molecules in a specific library by id.
   *
   * @param libraryId   a library identifier
   * @param moleculeIds an array of molecule identifiers
   * @return a list of molecules
   */
  List<Flattened.Molecule> findById(String libraryId, String... moleculeIds);

  /**
   * Count molecules in a library.
   *
   * @param libraryId a library identifier
   * @return count of molecules in a library
   */
  long countElements(String libraryId);

  /**
   * Update particular molecules in a library.
   *
   * @param libraryId a library identifier
   * @param molecules molecules to update
   * @return list of updated molecules
   */
  default List<Flattened.Molecule> updateMolecules(String libraryId, List<Molecule> molecules) {
    if (molecules == null || molecules.isEmpty()) {
      return List.of();
    }
    try (ItemWriter<Molecule> moleculeItemWriter = itemWriter(libraryId)) {
      molecules.forEach(moleculeItemWriter::write);
    }
    return findById(libraryId, molecules.stream().map(Molecule::getId).toArray(String[]::new));
  }

  /**
   * Delete particular molecules from a library.
   *
   * @param libraryId   a library identifier
   * @param moleculeIds molecule identifiers to delete
   * @return true if molecules were deleted, false otherwise
   */
  boolean deleteMolecules(String libraryId, List<String> moleculeIds);
}
