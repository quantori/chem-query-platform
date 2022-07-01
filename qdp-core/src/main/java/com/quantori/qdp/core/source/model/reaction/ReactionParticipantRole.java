package com.quantori.qdp.core.source.model.reaction;

/**
 * Reaction participants possible roles. Usually used to mark what expression represents.
 */
@SuppressWarnings("java:S115")
public enum ReactionParticipantRole {
  /**
   * It is the substance formed from chemical reactions.
   */
  product,
  /**
   * It is a substance that is present at the start of a chemical reaction.
   */
  reactant,
  /**
   * It is, a spectator ion is an ion that exists as a reactant and a product in a chemical equation.
   * Possible spectators are solvent and catalyst
   */
  spectator,
  /**
   * Substance might represent any of other types.
   */
  any,
  /**
   * Expression doesn't represent any of reaction part.
   * If entity has that role that means a molecule, not a reaction
   */
  none,
  /**
   * It is a substance that dissolves a solute, resulting in a solution.
   * Example of a spectator
   */
  solvent,
  /**
   * Catalyst is a substance that increases the rate of the process of a chemical reaction.
   * Example of a spectator
   */
  catalyst,
  /**
   * It is a process that leads to the chemical transformation of one set of chemical substances
   * to another.
   */
  reaction
}
