package com.quantori.qdp.core.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Search properties, added to a search to limit its results.
 *
 * <p>Consists of a property name, a property value and a logical operator that should be applied to
 * a property.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SearchProperty {
  private String property;
  private String logicalOperator;
  private String value;
}
