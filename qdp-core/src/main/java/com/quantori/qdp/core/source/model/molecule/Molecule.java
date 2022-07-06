package com.quantori.qdp.core.source.model.molecule;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Molecule {
  private String id;
  private Long serial;
  private String structure;
  private String decodedStructure;
  private String subHash;
  private List<Double> simHash;
  private String exactHash;
  private Map<String, String> properties;

  public Molecule(String id) {
    this.id = id;
  }

  @Override
  public String toString() {
    return "{ \"structure\":\"" + StringUtils.abbreviate(structure, 100) + "\", "
        + "\"sub\":\"" + StringUtils.abbreviate(subHash, 100) + "\", "
        + "\"sim\":" + (simHash != null ? StringUtils.abbreviate(simHash.toString(), 100) : "") + ", "
        + "\"exact\":\"" + StringUtils.abbreviate(exactHash, 100) + "\", "
        + "\"properties\":" + properties + "}";
  }
}