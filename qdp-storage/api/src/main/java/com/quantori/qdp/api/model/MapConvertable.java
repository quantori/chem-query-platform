package com.quantori.qdp.api.model;

import java.util.Map;

/**
 * Mark entity supports builtin conversions to {@link Map}.
 */
public interface MapConvertable {

  /**
   * Convert this object to map.
   *
   * @return object as map
   */
  Map<String, Object> toMap();

  /**
   * Populate item with data from map.
   *
   * @param map source of data
   * @throws IllegalArgumentException if unable to extract data, for example due type mismatch
   */
  void populate(Map<String, Object> map);
}
