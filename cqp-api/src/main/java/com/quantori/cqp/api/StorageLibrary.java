package com.quantori.cqp.api;

import com.quantori.cqp.api.model.Library;
import com.quantori.cqp.api.model.LibraryType;
import com.quantori.cqp.api.model.Property;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A storage must implement this interface to work with libraries. A library is a collection of molecules or reactions.
 * <p>
 * Before searching for molecules or reactions, one must create a library and upload data to it. Then a search can be
 * performed on a library. A search request accepts one or multiple library identifiers {@link Library#getId()} in which
 * the search must occur.
 */
public interface StorageLibrary {
  /**
   * Get {@code Library} by an identifier.
   *
   * @param libraryId a library identifier
   * @return a library wrapped in an {@link Optional}
   */
  Optional<Library> getLibraryById(String libraryId);

  /**
   * Get list of libraries by a name.
   *
   * @param libraryName a library name
   * @return list of libraries with the specified name
   */
  List<Library> getLibraryByName(String libraryName);

  /**
   * Get list of libraries by a type.
   *
   * @param libraryType a library type
   * @return list of libraries with the specified type
   */
  List<Library> getLibraryByType(LibraryType libraryType);

  Map<String, Property> getPropertiesMapping(String libraryId);

  /**
   * Create a library with a specified name, type, and custom storage specific parameters.
   *
   * @param libraryName   a library name
   * @param libraryType   a library type
   * @param serviceData   a map with custom storage specific parameters, i.e. they might be configuration of how to
   *                      shard a particular library. It is up to a particular storage implementation  to decide if
   *                      a storage accepts one or more such parameters.
   * @return created library
   * @throws LibraryAlreadyExistsException if a storage requires library name or both library name and library type to
   *                                       be unique that it might throw {@code LibraryAlreadyExistsException}
   */
  default Library createLibrary(String libraryName, LibraryType libraryType, Map<String, Object> serviceData)
    throws LibraryAlreadyExistsException {
    return createLibrary(libraryName, libraryType, Map.of(), serviceData);
  }

  /**
   * Create a library with a specified name, type, molecule properties meta, and custom storage specific parameters.
   *
   * @param libraryName       a library name
   * @param libraryType       a library type
   * @param propertiesMapping molecule properties meta information
   * @param serviceData       a map with custom storage specific parameters, i.e. they might be configuration of how to
   *                          shard a particular library. It is up to a particular storage implementation  to decide if
   *                          a storage accepts one or more such parameters.
   * @return created library
   * @throws LibraryAlreadyExistsException if a storage requires library name or both library name and library type to
   *                                       be unique that it might throw {@code LibraryAlreadyExistsException}
   */
  Library createLibrary(String libraryName, LibraryType libraryType, Map<String, Property> propertiesMapping,
                        Map<String, Object> serviceData) throws LibraryAlreadyExistsException;

  /**
   * Add new properties mapping to an existing library
   * @param libraryId         a library id
   * @param propertiesMapping molecule properties meta information
   */
  boolean addPropertiesMapping(String libraryId, Map<String, Property> propertiesMapping);

  /**
   * Update properties mapping in an existing library, all values except type can be changed, changing type won't have
   * any effect
   * @param libraryId         a library id
   * @param propertiesMapping molecule properties meta information to edit
   */
  boolean updatePropertiesMapping(String libraryId, Map<String, Property> propertiesMapping);

  /**
   * Delete a particular library.
   *
   * @param library a library to delete
   * @return true if a library was deleted, false otherwise
   */
  boolean deleteLibrary(Library library);

  /**
   * Update a particular library.
   *
   * @param library a library to update
   * @return updated library
   */
  Library updateLibrary(Library library);

  /**
   * In case a storage requires specific parameters to be set during library creation, it might provide the default
   * values for them.
   *
   * @return storage specific default parameters
   */
  Map<String, Object> getDefaultServiceData();
}
