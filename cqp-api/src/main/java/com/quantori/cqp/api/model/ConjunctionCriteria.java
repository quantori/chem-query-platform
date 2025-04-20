package com.quantori.cqp.api.model;

import com.quantori.cqp.core.model.Criteria;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A logical conjunction criteria providing logical "and" and logical "or" operations.
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode
public class ConjunctionCriteria implements Criteria {

  public static final Operator DEFAULT_OPERATOR = Operator.AND;

  private List<? extends Criteria> criteriaList;
  private Operator operator = DEFAULT_OPERATOR;

  /**
   * A simple view of criteria with two operands
   *
   * @param left     first (left) operand
   * @param right    second (right) operand
   * @param operator logical operator
   */
  public ConjunctionCriteria(Criteria left, Criteria right, Operator operator) {
    this.criteriaList = Arrays.asList(left, right);
    setOperator(operator);
  }

  public ConjunctionCriteria(List<? extends Criteria> criteriaList, Operator operator) {
    this.criteriaList = criteriaList;
    setOperator(operator);
  }

  public Criteria getLeft() {
    if (criteriaList != null && !criteriaList.isEmpty()) {
      return criteriaList.get(0);
    }
    return null;
  }

  public Criteria getRight() {
    if (criteriaList != null && criteriaList.size() > 1) {
      return criteriaList.get(1);
    }
    return null;
  }

  public void setOperator(Operator operator) {
    this.operator = Objects.requireNonNullElse(operator, DEFAULT_OPERATOR);
  }

  public enum Operator {
    AND, OR
  }
}
