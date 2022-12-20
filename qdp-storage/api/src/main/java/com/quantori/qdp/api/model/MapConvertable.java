package com.quantori.qdp.api.model;

import java.util.Map;

/**
 * A holder for additional storage parameters that might be required while creating a library. It is up to a storage
 * implementation to define and use these parameters.
 */
public interface MapConvertable {
  /**
   * Convert to {@code Map}
   *
   * @return a map of parameters
   */
  Map<String, Object> toMap();
}
