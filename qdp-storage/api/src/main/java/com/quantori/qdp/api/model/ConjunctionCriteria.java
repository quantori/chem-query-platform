package com.quantori.qdp.api.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * A logical conjunction criteria providing logical "and" and logical "or" operations.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConjunctionCriteria implements Criteria {

  private List<? extends Criteria> criteriaList;
  private Operator operator;

  /**
   * A simple view of criteria with two operands
   * @param left first (left) operand
   * @param right second (right) operand
   * @param operator logical operator
   */
  public ConjunctionCriteria(Criteria left, Criteria right, Operator operator) {
    this.criteriaList = List.of(left, right);
    this.operator = operator;
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

  public enum Operator {
    AND, OR
  }
}
