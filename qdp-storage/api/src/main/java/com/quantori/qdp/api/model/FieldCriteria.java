package com.quantori.qdp.api.model;

import java.util.Objects;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A field criteria providing field operations: less than, less than or equal, greater than, greater than or equal,
 * equal, not equal, empty, and non empty.
 */
@Data
@NoArgsConstructor
public class FieldCriteria implements Criteria {

  public static final Operator DEFAULT_OPERATOR = Operator.EQUAL;

  private String fieldName;
  private Operator operator = DEFAULT_OPERATOR;
  private String value;

  public FieldCriteria(String fieldName, Operator operator) {
    this.fieldName = fieldName;
    setOperator(operator);
  }

  public FieldCriteria(String fieldName, Operator operator, String value) {
    this.fieldName = fieldName;
    setOperator(operator);
    this.value = value;
  }

  public void setOperator(Operator operator) {
    this.operator = Objects.requireNonNullElse(operator, DEFAULT_OPERATOR);
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
