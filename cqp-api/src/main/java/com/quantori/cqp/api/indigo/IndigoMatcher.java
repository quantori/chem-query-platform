package com.quantori.cqp.api.indigo;

import com.epam.indigo.Indigo;
import com.epam.indigo.IndigoObject;
import com.quantori.cqp.api.model.ExactParams;
import com.quantori.cqp.api.model.SubstructureParams;
import com.quantori.cqp.api.service.ReactionsMatcher;
import org.apache.commons.lang3.StringUtils;

import java.util.function.BinaryOperator;

/**
 * An indigo implementation of molecules and reactions matcher.
 * <p>
 * See {@link com.quantori.cqp.api.service.MoleculesMatcher} and {@link ReactionsMatcher}
 * <p>
 * Currently, this implementation is shared for all supported storage types.
 */
public class  IndigoMatcher implements ReactionsMatcher {

  private static final int CARBON_ATOMIC_NUMBER = 6;

  private final IndigoProvider indigoProvider;

  public IndigoMatcher(IndigoProvider indigoProvider) {

    this.indigoProvider = indigoProvider;
  }

  private static String clearAAMAndFoldHydrogens(String reaction, Indigo indigo) {

    IndigoObject indigoObject = indigo.loadReaction(reaction);
    try {
      indigoObject.foldHydrogens();
      indigoObject.clearAAM();
      return indigoObject.smiles();
    } finally {
      dispose(indigoObject);
    }
  }

  @Override
  public boolean isExactMatch(byte[] structure, ExactParams exactParams) {

    Indigo indigo = indigoProvider.take();
    try {
      IndigoObject target = indigo.deserialize(structure);
      try {
        IndigoObject query = indigo.loadMolecule(exactParams.getSearchQuery());
        try {
          return isIndigoMatch(target, query, false,
              (q, t) -> indigo.exactMatch(t, q, getExactSearchFlags(exactParams)));
        } finally {
          dispose(query);
        }
      } finally {
        dispose(target);
      }
    } finally {
      indigoProvider.offer(indigo);
    }
  }

  @Override
  public boolean isSubstructureMatch(byte[] structure, SubstructureParams substructureParams) {

    if (StringUtils.isBlank(substructureParams.getSearchQuery())) {
      return true;
    }

    Indigo indigo = indigoProvider.take();

    try {
      IndigoObject target = indigo.deserialize(structure);
      try {
        IndigoObject query = indigo.loadQueryMolecule(substructureParams.getSearchQuery());
        try {
          return isIndigoMatch(target, query, substructureParams.isHeteroatoms(),
              (q, t) -> indigo.substructureMatcher(t).match(q));
        } finally {
          dispose(query);
        }
      } finally {
        dispose(target);
      }
    } finally {
      indigoProvider.offer(indigo);
    }
  }

  @Override
  public boolean isReactionSubstructureMatch(
      String reaction,
      SubstructureParams substructureParams
  ) {
    var searchQuery = substructureParams.getSearchQuery();
    var heteroatoms = substructureParams.isHeteroatoms();
    return StringUtils.isBlank(searchQuery)
      || isRegularReactionSubstructureMatch(reaction, searchQuery, heteroatoms)
      || isReactionSubstructureMatchAutomap(reaction, searchQuery, heteroatoms);
  }

  private boolean isRegularReactionSubstructureMatch(
      String reaction,
      final String queryReaction,
      boolean onHeteroAtoms
  ) {
    Indigo indigo = indigoProvider.take();

    try {
      IndigoObject indigoObject = indigo.loadReaction(queryReaction);
      try {
        indigoObject.foldHydrogens();
        indigoObject.aromatize();
        IndigoObject query = indigo.loadQueryReaction(indigoObject.smiles());
        try {
          IndigoObject target = indigo.loadReaction(reaction);
          try {
            return isIndigoMatch(target, query, onHeteroAtoms, (q, t) -> indigo.substructureMatcher(t).match(q));
          } finally {
            dispose(target);
          }
        } finally {
          dispose(query);
        }
      } finally {
        dispose(indigoObject);
      }
    } finally {
      indigoProvider.offer(indigo);
    }
  }

  private boolean isReactionSubstructureMatchAutomap(String reaction, String queryReaction,
                                                     boolean onHeteroAtoms) {

    Indigo indigo = indigoProvider.take();
    try {
      IndigoObject query = indigo.loadQueryReaction(clearAAMAndFoldHydrogens(queryReaction, indigo));
      try {
        query.aromatize();
        query.automap();
        IndigoObject target = indigo.loadReaction(clearAAMAndFoldHydrogens(reaction, indigo));
        try {
          target.aromatize();
          return isIndigoMatch(target, query, onHeteroAtoms, (q, t) -> indigo.substructureMatcher(t).match(q));
        } finally {
          dispose(target);
        }
      } finally {
        dispose(query);
      }
    } finally {
      indigoProvider.offer(indigo);
    }
  }

  private boolean isIndigoMatch(IndigoObject target, IndigoObject query, boolean onHeteroAtoms,
                                BinaryOperator<IndigoObject> matcherFunction) {
    if (onHeteroAtoms) {
      for (var atom : query.iterateAtoms()) {
        if (!atom.isPseudoatom() && atom.atomicNumber() == CARBON_ATOMIC_NUMBER) {
          atom.addConstraint("substituents", "" + atom.degree());
        }
      }
    }
    query.aromatize();
    target.aromatize();
    var match = matcherFunction.apply(query, target);
    try {
      return match != null;
    } finally {
      dispose(match);
    }
  }

  private String getExactSearchFlags(ExactParams exactParams) {
    if (StringUtils.startsWith(exactParams.getSearchQuery(), "InChI")) {
      return "NONE";
    } else {
      return "ALL";
    }
  }

  private static void dispose(IndigoObject indigoObject) {
    if (indigoObject != null) {
      indigoObject.dispose();
    }
  }
}
