package com.quantori.qdp.core.source;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.quantori.qdp.core.model.StorageUploadItem;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, defaultImpl = TestMolecule.class)
public class TestStorageUploadItem implements StorageUploadItem {
  private String id;
}
