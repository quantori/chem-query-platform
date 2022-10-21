package com.quantori.qdp.core.source;

import com.quantori.qdp.core.source.model.StorageItem;

public  class TestStorageItem implements StorageItem {
  private final int number;

  public TestStorageItem(int i) {
    this.number = i;
  }

  public int getNumber() {
    return number;
  }
}
