package com.quantori.qdp.api.model.upload;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.quantori.qdp.api.model.core.StorageUploadItem;
import java.time.Instant;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


/**
 * Generic definition of molecule structure.
 */
@Getter
@Setter
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, defaultImpl = Molecule.class)
public class Molecule extends BasicMolecule implements StorageUploadItem {
  private byte[] sim;
  private String exactHash;
  private Map<String, String> molProperties;
  private Long customOrder;
  private Instant createdStamp;
  private Instant updatedStamp;

  public Map<String, String> getMolProperties() {
    if (molProperties == null) {
      return Map.of();
    }
    return molProperties;
  }
}
