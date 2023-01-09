package com.quantori.qdp.core.source;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.quantori.qdp.api.model.core.SearchItem;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@AllArgsConstructor
public class TestSearchItem implements SearchItem {
  private String id;

  @JsonCreator
  public TestSearchItem(int id) {
    this.id = Integer.toString(id);
  }

}