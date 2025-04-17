package com.quantori.cqp.storage.elasticsearch.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.experimental.FieldNameConstants;

@Data
@FieldNameConstants
public abstract class BasicMoleculeDocument {
  @JsonProperty("id")
  private String id;

  @JsonProperty("smiles")
  private String smiles;

  @JsonProperty("structure")
  private String structure;

  @JsonProperty("exact")
  private String exactHash;

  @JsonProperty("sub")
  private String subHash;
}
