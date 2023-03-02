package com.quantori.qdp.api.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class SortParams {

  private final List<Sort> sortList;

  public record Sort(String fieldName, Order order, Type type) {
  }

  public enum Type {GENERAL, NESTED}

  public enum Order {ASC, DESC}
}
