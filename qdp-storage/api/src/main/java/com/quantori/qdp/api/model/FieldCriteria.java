package com.quantori.qdp.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A field criteria providing field operations: less than, less than or equal, greater than, greater than or equal,
 * equal, not equal, empty, and non empty.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class FieldCriteria implements Criteria {
  private String fieldName;
  private Operator operator;
  private String value;

  public FieldCriteria(String fieldName, Operator operator) {
    this.fieldName = fieldName;
    this.operator = operator;
  }

  public enum Operator {
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    EQUAL,
    NONEMPTY,
    EMPTY
  }

}
