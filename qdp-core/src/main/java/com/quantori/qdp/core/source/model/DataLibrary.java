package com.quantori.qdp.core.source.model;

import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@Value
@Builder
@RequiredArgsConstructor
public class DataLibrary {
  String name;
  DataLibraryType type;
  Map<String, Object> properties;

  public DataLibrary(DataLibrary library) {
    this.name = library.name;
    this.type = library.type;
    this.properties = library.properties != null ? new HashMap<>(library.properties) : new HashMap<>();
  }
}
