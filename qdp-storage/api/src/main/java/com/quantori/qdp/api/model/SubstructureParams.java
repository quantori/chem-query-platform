package com.quantori.qdp.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SubstructureParams {
  private String searchQuery;
  private boolean heteroatoms;
}
