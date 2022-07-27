package com.quantori.qdp.core.source;

import com.quantori.qdp.core.source.model.SearchItem;
import com.quantori.qdp.core.source.model.StorageItem;

public class Molecule implements StorageItem, SearchItem {
  private String id;

  public Molecule() {
  }

  public Molecule(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}
