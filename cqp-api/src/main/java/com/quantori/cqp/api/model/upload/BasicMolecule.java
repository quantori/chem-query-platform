package com.quantori.cqp.api.model.upload;

import lombok.Data;

@Data
public class BasicMolecule {
  private String id;
  private String smiles;
  private byte[] structure;
  private byte[] exact;
  private byte[] sub;
}
