package com.quantori.cqp.storage.elasticsearch.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.quantori.cqp.api.model.LibraryType;
import com.quantori.cqp.api.model.Property;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;

import java.util.Map;

@Data
@NoArgsConstructor
@FieldNameConstants
public class LibraryDocument {

  @JsonProperty("name")
  private String name;

  @JsonProperty("type")
  private String type;

  @JsonProperty("size")
  private long size;

  @JsonProperty("created_timestamp")
  private Long createdStamp;

  @JsonProperty("updated_timestamp")
  private Long updatedStamp;

  @JsonProperty("properties_mapping")
  private Map<String, Property> propertiesMapping;

  public LibraryDocument(String name, LibraryType type, Map<String, Property> propertiesMapping) {
    this.name = name;
    this.type = type.toString();
    this.propertiesMapping = propertiesMapping;
    size = 0;
  }
}
