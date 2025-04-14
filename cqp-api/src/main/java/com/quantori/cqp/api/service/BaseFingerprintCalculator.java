package com.quantori.cqp.api.service;

public interface BaseFingerprintCalculator {

  byte[] exactFingerprint(String structure);

  byte[] exactFingerprint(byte[] structure);

  byte[] substructureFingerprint(String structure);

  byte[] substructureFingerprint(byte[] structure);
}
