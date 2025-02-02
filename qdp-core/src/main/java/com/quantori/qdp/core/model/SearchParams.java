package com.quantori.qdp.core.model;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

/**
 * An object to store parameters of exact search for molecules and reactions.
 */
@SuperBuilder
@Getter
@Setter
@ToString
@EqualsAndHashCode
public abstract class SearchParams {
  private String searchQuery;
}
