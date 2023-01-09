package com.quantori.qdp.core.source;

import com.quantori.qdp.api.model.core.StorageUploadItem;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString
public class TestStorageUploadItem implements StorageUploadItem {
  private String id;
}
