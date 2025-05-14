package com.quantori.cqp.api;

public interface BaseFingerprintCalculator {

  byte[] exactFingerprint(String structure);

  byte[] exactFingerprint(byte[] structure);

  byte[] substructureFingerprint(String structure);

  byte[] substructureFingerprint(byte[] structure);
}
