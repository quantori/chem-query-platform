package com.quantori.cqp.api;

import com.quantori.cqp.api.model.MapConvertable;

/**
 * This interface represents a configuration for a storage that should be supported by CQP.
 * <p>
 * Any storage must support working with three types of items:
 * <ul>
 *   <li>Libraries {@link StorageLibrary}
 *   <li>Molecules {@link StorageMolecules}
 *   <li>Reactions {@link StorageReactions}
 * </ul><p>
 * To refer to a storage it needs to define a unique storage type {@link #storageType} as well as
 * it needs to specify storage specific parameters in a key / value format {@link #defaultServiceData}.
 */
public interface StorageConfiguration {
  /**
   * Returns a service containing logic to work with libraries.
   *
   * @return a {@link StorageLibrary} containing logic to work with libraries
   */
  StorageLibrary getStorageLibrary();

  /**
   * Returns a service containing logic to work with molecules.
   *
   * @return a {@link StorageMolecules} containing logic to work with molecules
   */
  StorageMolecules getStorageMolecules();

  /**
   * Returns a service containing logic to work with reactions.
   *
   * @return a {@link StorageReactions} containing logic to work with reactions
   */
  StorageReactions getStorageReactions();

  /**
   * Returns a unique human readable identifier of a storage.
   *
   * @return a unique human readable identifier of a storage
   */
  String storageType();

  /**
   * Returns a configuration to set up and run a storage in a key / value way.
   *
   * @return a {@link MapConvertable} containing all key / value records to set up and run a storage
   */
  MapConvertable defaultServiceData();
}
