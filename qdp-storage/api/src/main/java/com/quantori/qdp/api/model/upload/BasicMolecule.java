package com.quantori.qdp.api.model.upload;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class BasicMolecule {
  private String id;
  private String smiles;
  private byte[] structure;
  private byte[] exact;
  private byte[] sub;
}
