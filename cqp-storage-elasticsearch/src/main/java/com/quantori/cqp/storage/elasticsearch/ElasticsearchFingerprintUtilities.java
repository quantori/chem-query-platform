package com.quantori.cqp.storage.elasticsearch;

import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@UtilityClass
public class ElasticsearchFingerprintUtilities {

  public static final int FINGERPRINT_SIZE = 512;

  public static final List<Double> ZERO_LIST =
      new ArrayList<>(Collections.nCopies(FINGERPRINT_SIZE, 0.0));

  public static List<Double> getSimilarityHash(byte[] data) {
    List<Double> fingerPrint = oneBitsList(data);
    while (fingerPrint.size() < FINGERPRINT_SIZE) {
      fingerPrint.add(0.0);
    }
    return fingerPrint;
  }

  private static List<Double> oneBitsList(byte[] data) {
    List<Double> res = new ArrayList<>();
    for (byte datum : data) {
      for (var mask = 0x01; mask != 0x100; mask <<= 1) {
        if ((datum & mask) != 0) {
          res.add(1.0);
        } else {
          res.add(0.0);
        }
      }
    }
    return res;
  }

  public static double numberOfNonZeroElements(List<Double> simHash) {
    return simHash.stream().filter(ElasticsearchFingerprintUtilities::isDoubleNonZero).count();
  }

  private static boolean isDoubleNonZero(Double elem) {
    return Math.abs(elem) > 0.5;
  }
}
