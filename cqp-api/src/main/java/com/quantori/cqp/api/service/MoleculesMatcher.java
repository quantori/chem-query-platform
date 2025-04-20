package com.quantori.cqp.api.service;


import com.quantori.cqp.core.model.ExactParams;
import com.quantori.cqp.core.model.SubstructureParams;

/**
 * A molecules matcher for exact and substructure search.
 * <p>
 * Checks a molecule structure against search params to validate that a molecule matches the search and should be
 * returned to a user as part of  a user's search request.
 */
public interface MoleculesMatcher {

  /**
   * Checks a molecule against exact search params.
   *
   * @param structure   a molecule structure
   * @param exactParams search parameters
   * @return true is a molecule matches, otherwise false
   */
  boolean isExactMatch(byte[] structure, ExactParams exactParams);

  /**
   * Checks a molecule against substructure search params.
   *
   * @param structure          a molecule structure
   * @param substructureParams search parameters
   * @return true is a molecule matches, otherwise false
   */
  boolean isSubstructureMatch(byte[] structure, SubstructureParams substructureParams);

}
