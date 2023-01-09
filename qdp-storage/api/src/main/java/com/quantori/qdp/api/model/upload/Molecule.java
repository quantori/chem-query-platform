package com.quantori.qdp.api.model.upload;

import com.quantori.qdp.api.model.core.StorageUploadItem;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * Generic definition of molecule structure.
 */
@Data
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class Molecule extends BasicMolecule implements StorageUploadItem {
  private byte[] sim;
  private Map<String, String> molProperties;
}
