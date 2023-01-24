package com.quantori.qdp.core.source;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.quantori.qdp.api.model.core.SearchItem;
import com.quantori.qdp.api.model.upload.Molecule;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@AllArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, defaultImpl = Molecule.class)
public class TestSearchItem implements SearchItem {
  private String id;

  @JsonCreator
  public TestSearchItem(int id) {
    this.id = Integer.toString(id);
  }

}