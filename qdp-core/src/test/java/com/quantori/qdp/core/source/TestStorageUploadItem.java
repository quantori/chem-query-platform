package com.quantori.qdp.core.source;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.quantori.qdp.api.model.core.StorageUploadItem;
import com.quantori.qdp.api.model.upload.Molecule;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, defaultImpl = Molecule.class)
public class TestStorageUploadItem implements StorageUploadItem {
  private String id;
}
