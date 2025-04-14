package com.quantori.cqp.api.indigo;

import com.epam.indigo.Indigo;
import com.epam.indigo.IndigoObject;

import com.quantori.cqp.api.service.MoleculesFingerprintCalculator;
import com.quantori.cqp.api.service.ReactionsFingerprintCalculator;
import com.quantori.cqp.api.util.FingerPrintUtilities;
import org.apache.commons.lang3.StringUtils;

/**
 * An indigo implementation of fingerprint molecules and reactions calculator.
 */
public class IndigoFingerprintCalculator implements MoleculesFingerprintCalculator, ReactionsFingerprintCalculator {

  private static final byte[] EMPTY_FINGERPRINT = new byte[0];

  private final IndigoProvider indigoProvider;

  public IndigoFingerprintCalculator(IndigoProvider indigoProvider) {

    this.indigoProvider = indigoProvider;
  }

  @Override
  public byte[] exactFingerprint(String structure) {

    if (isEmptyStructure(structure)) {
      return EMPTY_FINGERPRINT;
    }

    Indigo indigo = indigoProvider.take();
    try {
      var molecule = indigo.loadMolecule(structure);
      try {
        return getExactFingerprint(molecule);
      } finally {
        dispose(molecule);
      }
    } finally {
      indigoProvider.offer(indigo);
    }
  }

  @Override
  public byte[] exactFingerprint(byte[] structure) {

    if (isEmptyStructure(structure)) {
      return EMPTY_FINGERPRINT;
    }

    Indigo indigo = indigoProvider.take();
    try {
      var molecule = indigo.loadMolecule(structure);
      try {
        return getExactFingerprint(molecule);
      } finally {
        dispose(molecule);
      }
    } finally {
      indigoProvider.offer(indigo);
    }
  }

  @Override
  public byte[] substructureFingerprint(String structure) {

    if (isEmptyStructure(structure)) {
      return EMPTY_FINGERPRINT;
    }

    Indigo indigo = indigoProvider.take();
    try {
      var molecule = indigo.loadMolecule(structure);
      try {
        return getSubstructureFingerprint(molecule);
      } finally {
        dispose(molecule);
      }
    } finally {
      indigoProvider.offer(indigo);
    }
  }

  @Override
  public byte[] substructureFingerprint(byte[] structure) {

    if (isEmptyStructure(structure)) {
      return EMPTY_FINGERPRINT;
    }

    Indigo indigo = indigoProvider.take();
    try {
      var molecule = indigo.loadMolecule(structure);
      try {
        return getSubstructureFingerprint(molecule);
      } finally {
        dispose(molecule);
      }
    } finally {
      indigoProvider.offer(indigo);
    }
  }

  @Override
  public byte[] similarityFingerprint(String structure) {

    if (isEmptyStructure(structure)) {
      return EMPTY_FINGERPRINT;
    }

    Indigo indigo = indigoProvider.take();
    try {
      var molecule = indigo.loadMolecule(structure);
      try {
        return FingerPrintUtilities.getSimilarityFingerprint512(getSimilarityFingerprint(molecule));
      } finally {
        dispose(molecule);
      }
    } finally {
      indigoProvider.offer(indigo);
    }
  }

  @Override
  public byte[] similarityFingerprint(byte[] structure) {

    if (isEmptyStructure(structure)) {
      return EMPTY_FINGERPRINT;
    }

    Indigo indigo = indigoProvider.take();
    try {
      var molecule = indigo.loadMolecule(structure);
      try {
        return FingerPrintUtilities.getSimilarityFingerprint512(getSimilarityFingerprint(molecule));
      } finally {
        dispose(molecule);
      }
    } finally {
      indigoProvider.offer(indigo);
    }
  }

  @Override
  public byte[] substructureReactionFingerprint(String structure) {

    if (isEmptyStructure(structure)) {
      return EMPTY_FINGERPRINT;
    }

    Indigo indigo = indigoProvider.take();
    try {
      var reaction = indigo.loadQueryReaction(structure);
      try {
        return getSubstructureReactionFingerprint(reaction);
      } finally {
        dispose(reaction);
      }
    } finally {
      indigoProvider.offer(indigo);
    }
  }

  @Override
  public byte[] substructureReactionFingerprint(byte[] structure) {

    if (isEmptyStructure(structure)) {
      return EMPTY_FINGERPRINT;
    }

    Indigo indigo = indigoProvider.take();
    try {
      var reaction = indigo.loadQueryReaction(structure);
      try {
        return getSubstructureReactionFingerprint(reaction);
      } finally {
        dispose(reaction);
      }
    } finally {
      indigoProvider.offer(indigo);
    }
  }

  private boolean isEmptyStructure(byte[] structure) {

    return structure == null || structure.length == 0;
  }

  private boolean isEmptyStructure(String structure) {
    return StringUtils.isBlank(structure);
  }

  private byte[] getExactFingerprint(IndigoObject molecule) {

    molecule.aromatize();
    var badValence = molecule.checkBadValence();
    var type = badValence.isEmpty() ? "full" : "sub";
    return molecule.fingerprint(type).toBuffer();
  }

  private byte[] getSubstructureFingerprint(IndigoObject molecule) {

    molecule.aromatize();
    return molecule.fingerprint("sub").toBuffer();
  }

  private byte[] getSimilarityFingerprint(IndigoObject molecule) {

    molecule.aromatize();
    return molecule.fingerprint("sim").toBuffer();
  }

  private byte[] getSubstructureReactionFingerprint(IndigoObject reaction) {

    reaction.aromatize();
    return reaction.fingerprint("sub").toBuffer();
  }

  private static void dispose(IndigoObject indigoObject) {
    if (indigoObject != null) {
      indigoObject.dispose();
    }
  }
}
