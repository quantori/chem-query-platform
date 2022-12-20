package com.quantori.qdp.api.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * An object to store parameters of exact search for molecules and reactions.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ExactParams {
  private String searchQuery;
}
