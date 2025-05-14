package com.quantori.cqp.api;

/**
 * A fingerprint calculator for molecules. It provides default implementation for all fingerprints but can be
 * customized if needed.
 */
public interface MoleculesFingerprintCalculator extends BaseFingerprintCalculator {

  byte[] similarityFingerprint(String structure);

  byte[] similarityFingerprint(byte[] structure);
}
