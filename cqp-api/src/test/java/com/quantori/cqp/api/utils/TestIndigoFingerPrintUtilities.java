package com.quantori.cqp.api.utils;

import com.epam.indigo.Indigo;
import com.epam.indigo.IndigoObject;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@UtilityClass
public class TestIndigoFingerPrintUtilities {
  private static final Indigo indigo = new Indigo();

  public static byte[] getSubstructureBinaryFingerprint(
      String moleculeDescription, List<Pair<String, String>> options) {
    setOptions(options);
    var molecule = indigo.loadQueryMolecule(moleculeDescription);
    return getSubstructureBinaryFingerprint(molecule);
  }

  public static byte[] getSubstructureBinaryFingerprint(String moleculeDescription) {
    var molecule = indigo.loadMolecule(moleculeDescription);
    return getSubstructureBinaryFingerprint(molecule);
  }

  public static byte[] getSubstructureBinaryFingerprint(final IndigoObject structure) {
    Optional<IndigoObject> formulaStructure = processMolecule(structure);
    var structureResolved = formulaStructure.orElse(structure);
    structureResolved.aromatize();
    return structureResolved.fingerprint("sub").toBuffer();
  }

  public static byte[] getSimilarityBinaryFingerprint512(IndigoObject molecule) {
    molecule.aromatize();
    return getSimilarityBinaryFingerprint512(molecule.fingerprint("sim").toBuffer());
  }

  public static byte[] getSimilarityBinaryFingerprint512(byte[] data) {
    return Arrays.copyOfRange(data, 25 * 8, 25 * 8 + 8 * 8);
  }

  public static byte[] getSubstructureBinaryFingerprintOfReaction(
      String reaction, List<Pair<String, String>> options) {
    setOptions(options);
    var molecule = indigo.loadQueryReaction(reaction);
    return getSubstructureBinaryFingerprint(molecule);
  }

  public static byte[] getExactBinaryFingerprint(String moleculeDescription) {
    var molecule = indigo.loadMolecule(moleculeDescription);
    Optional<IndigoObject> formulaMolecule = processMolecule(molecule);
    molecule = formulaMolecule.orElse(molecule);
    return getExactBinaryFingerprint(molecule);
  }

  public static byte[] getExactBinaryFingerprint(byte[] moleculeDescription) {
    var molecule = indigo.loadMolecule(moleculeDescription);
    Optional<IndigoObject> formulaMolecule = processMolecule(molecule);
    molecule = formulaMolecule.orElse(molecule);
    return getExactBinaryFingerprint(molecule);
  }

  public static byte[] getExactBinaryFingerprint(final IndigoObject molecule) {
    molecule.aromatize();
    var badValence = molecule.checkBadValence();
    var type = badValence.isEmpty() ? "full" : "sub";
    return molecule.fingerprint(type).toBuffer();
  }

  public static String deserializeMoleculeIntoSmile(byte[] structure) {
    if (structure == null || structure.length == 0) {
      return null;
    }

    var indigo = new Indigo();

    var molFile = indigo.deserialize(structure);

    var checkBadValence = molFile.checkBadValence();
    if (checkBadValence.isEmpty()) {
      molFile.foldHydrogens();
    }
    return molFile.smiles();
  }

  private static Optional<IndigoObject> processMolecule(final IndigoObject molecule) {
    String formula;
    IndigoObject indigoObject;
    try {
      formula = molecule.getProperty("FORMULA");
      indigoObject = indigo.loadQueryMolecule(formula);
    } catch (Exception ignored) {
      return Optional.empty();
    }
    if (formula != null) {
      for (var prop : molecule.iterateProperties()) {
        indigoObject.setProperty(prop.name(), prop.rawData());
      }
    }
    return Optional.of(indigoObject);
  }

  private static void setOptions(List<Pair<String, String>> options) {
    if (options == null) {
      return;
    }
    for (Pair<String, String> pair : options) {
      indigo.setOption(pair.getLeft(), pair.getRight());
    }
  }
}
