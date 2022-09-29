package com.quantori.qdp.storage.api;

import com.quantori.qdp.storage.solr.SolrServiceData;
import java.util.List;
import java.util.Optional;

public interface StorageLibrary {
  Optional<Library> getLibraryById(String libraryId);

  List<Library> getLibraryByName(String libraryName);

  List<Library> getLibraryByType(LibraryType libraryType);

  Library createLibrary(String libraryName, LibraryType libraryType, SolrServiceData serviceData)
      throws LibraryAlreadyExistsException;

  boolean deleteLibrary(Library library);

  void updateLibrarySize(String libraryId, long count);
}
