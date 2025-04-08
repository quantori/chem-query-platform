package com.quantori.cqp.api.service;


import com.quantori.cqp.api.model.SubstructureParams;

/**
 * A reactions matcher for exact and substructure search.
 * <p>
 * Checks a reaction and a reaction participant structure against search params to validate that a reaction matches
 * the search and should be returned to a user as part of  a user's search request.
 */
public interface ReactionsMatcher extends MoleculesMatcher {

  /**
   * Checks a reaction (i.e. reaction smiles) against substructure search params.
   *
   * @param reaction           reaction structure
   * @param substructureParams search parameters
   * @return true is a reaction matches, otherwise false
   */
  boolean isReactionSubstructureMatch(String reaction, SubstructureParams substructureParams);

}
