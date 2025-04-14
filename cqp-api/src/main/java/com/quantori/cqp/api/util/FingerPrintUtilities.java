package com.quantori.cqp.api.util;

import lombok.experimental.UtilityClass;
import org.apache.commons.codec.digest.DigestUtils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.BitSet;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Set of utilities to work with fingerprints.
 */
@UtilityClass
public final class FingerPrintUtilities {

  /**
   * Check that a fingerprint is not empty or contains non-zero bytes.
   *
   * @param arr fingerprint
   * @return true if fingerptint is not empty, otherwise false
   */
  public static boolean isNotBlank(byte[] arr) {
    for (byte b : arr) {
      if (b != 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get an exact hash of a fingerprint.
   *
   * @param exact a fingerprint
   * @return an exact hash
   */
  public static String exactHash(byte[] exact) {
    return DigestUtils.md5Hex(exact).toUpperCase(Locale.ROOT);
  }


  /**
   * Get a substructure hash of a fingerprint.
   *
   * @param bytea a fingerprint
   * @return a substructure hash
   */
  public static String substructureHash(byte[] bytea) {
    return BitSet.valueOf(bytea).stream()
      .mapToObj(String::valueOf)
      .collect(Collectors.joining(" "));
  }

  /**
   * Encode a byte array molecule structure to a string.
   *
   * @param bytea a byte array molecule structure
   * @return a string representation of a byte array molecule structure
   */
  public static String encodeStructure(byte[] bytea) {
    return encodeBase64(bytea);
  }

  /**
   * Decode a string representation of a molecule structure to a byte array
   *
   * @param source a string representation of a byte array molecule structure
   * @return a byte array molecule structure
   */
  public static byte[] decodeStructure(String source) {
    return Base64.getDecoder().decode(source.getBytes(StandardCharsets.US_ASCII));
  }

  /**
   * Encode a byte array molecule sim hash to a string.
   *
   * @param bytea a byte array molecule sim hash
   * @return a string representation of a byte array molecule sim hash
   */
  public static String encodeSim(byte[] bytea) {
    return encodeBase64(bytea);
  }

  private static String encodeBase64(byte[] bytea) {
    return new String(Base64.getEncoder().encode(bytea), StandardCharsets.US_ASCII);
  }

  public static byte[] getSimilarityFingerprint512(byte[] data) {
    return Arrays.copyOfRange(data, 25 * 8, 25 * 8 + 8 * 8);
  }
}
