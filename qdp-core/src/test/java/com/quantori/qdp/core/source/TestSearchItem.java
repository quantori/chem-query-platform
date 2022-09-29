package com.quantori.qdp.core.source;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.quantori.qdp.core.source.model.SearchItem;

public class TestSearchItem implements SearchItem {
  private final String number;

  @JsonCreator
  public TestSearchItem(int number) {
    this.number = Integer.toString(number);
  }

  public String getNumber() {
    return number;
  }

  @Override
  public String toString() {
    return "SearchItem{" +
        "number='" + number + '\'' +
        '}';
  }
}