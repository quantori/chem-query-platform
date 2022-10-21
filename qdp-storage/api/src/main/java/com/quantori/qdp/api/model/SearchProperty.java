package com.quantori.qdp.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SearchProperty {
  private String property;
  private String logicalOperator;
  private String value;
}
