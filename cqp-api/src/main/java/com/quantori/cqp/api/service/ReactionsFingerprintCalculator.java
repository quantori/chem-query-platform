package com.quantori.cqp.api.service;

/**
 * A fingerprint calculator for reactions. It provides default implementation for all fingerprints but can be
 * customized if needed.
 */
public interface ReactionsFingerprintCalculator extends BaseFingerprintCalculator {

  byte[] substructureReactionFingerprint(String structure);

  byte[] substructureReactionFingerprint(byte[] structure);
}
