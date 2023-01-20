package com.quantori.qdp.api.model.upload;

import com.quantori.qdp.api.model.core.StorageUploadItem;
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
public class Molecule extends BasicMolecule implements StorageUploadItem {
  private byte[] sim;
  private Map<String, String> molProperties;
}
