package com.quantori.qdp.core.source.model.molecule;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * This is raw molecule structure with binary fingerprints.
 */


@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class MoleculeFingerprint {
  String id;
  String smile;
  byte[] structure;
  byte[] sub;
  byte[] sim;
  byte[] exact;
}
