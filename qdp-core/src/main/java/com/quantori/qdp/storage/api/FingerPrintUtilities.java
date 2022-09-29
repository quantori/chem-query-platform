package com.quantori.qdp.storage.api;

import com.epam.indigo.Indigo;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.BitSet;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;

@Slf4j
@UtilityClass
public class FingerPrintUtilities {
  private static final ThreadLocal<Indigo> indigoThreadLocal = ThreadLocal.withInitial(Indigo::new);

  public static boolean isNotBlank(byte[] arr) {
    for (byte b : arr) {
      if (b != 0) {
        return true;
      }
    }
    return false;
  }

  public static String similarityHash(byte[] bytea) {
    char[] word = new char[bytea.length * 8];
    int cnt = 0;
    for (byte b : bytea) {
      for (var mask = 0x01; mask < 0x100; mask <<= 1) {
        word[cnt++] = (b & mask) == 0 ? '0' : '1';
      }
    }
    return String.valueOf(word);
  }

  public static String substructureHash(byte[] bytea) {
    return BitSet.valueOf(bytea).stream()
        .mapToObj(String::valueOf)
        .collect(Collectors.joining(" "));
  }

  public static String exactHash(byte[] data) {
    Optional<MessageDigest> md = tryToCreateHashFunction();
    if (md.isPresent()) {
      var messageDigest = md.get();
      messageDigest.update(data);
      return Hex.encodeHexString(messageDigest.digest());
    }
    return null;
  }

  private static Optional<MessageDigest> tryToCreateHashFunction() {
    final var algorithm = "MD5";
    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance(algorithm);
    } catch (NoSuchAlgorithmException e) {
      // ignore
    }
    return Optional.ofNullable(md);
  }

  public static String decodeStructureIntoSmiles(String molecule, boolean hydrogenVisible) {
    if (molecule == null) {
      return "NONE";
    }
    var decoder = Base64.getDecoder();
    byte[] decode = decoder.decode(molecule.getBytes(StandardCharsets.US_ASCII));
    return deserializeMoleculeIntoSmile(decode, hydrogenVisible);
  }

  private static String deserializeMoleculeIntoSmile(byte[] decode, boolean hydrogenVisible) {
    var indigo = indigoThreadLocal.get();

    log.trace("Indigo library deserialize method call with parameters: {}", decode);
    var molFile = indigo.deserialize(decode);
    log.trace("Indigo library deserialize method returns result: {}", molFile);

    var checkBadValence = molFile.checkBadValence();
    if (checkBadValence.isEmpty()) {
      if (!hydrogenVisible) {
        molFile.foldHydrogens();
      } else {
        molFile.unfoldHydrogens();
      }
    } else {
      log.warn("Invalid molecule, {}", checkBadValence);
    }
    return molFile.smiles();
  }
}
