package com.quantori.cqp.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class Property {
  private String name;
  private PropertyType type;
  private int position;
  private boolean hidden;
  private boolean deleted;

  public Property(String name, PropertyType type) {
    this.name = name;
    this.type = type;
  }

  public enum PropertyType {
    STRING, DECIMAL, DATE
  }
}
