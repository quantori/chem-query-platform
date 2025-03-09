package com.quantori.cqp.core.source;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.quantori.cqp.core.model.SearchItem;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@AllArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, defaultImpl = TestMolecule.class)
public class TestSearchItem implements SearchItem {
  private String id;

  @JsonCreator
  public TestSearchItem(int id) {
    this.id = Integer.toString(id);
  }
}
