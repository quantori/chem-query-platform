package com.quantori.qdp.core.source;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.quantori.qdp.api.model.core.StorageItem;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@ToString
public class TestStorageItem implements StorageItem {
  private String id;

  @JsonCreator
  public TestStorageItem(int id) {
    this.id = Integer.toString(id);
  }

}