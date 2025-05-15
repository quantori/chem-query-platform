package com.quantori.cqp.core.source;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.quantori.cqp.api.StorageItem;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@ToString
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, defaultImpl = TestMolecule.class)
public class TestStorageItem implements StorageItem {
  private String id;

  @JsonCreator
  public TestStorageItem(int id) {
    this.id = Integer.toString(id);
  }
}
