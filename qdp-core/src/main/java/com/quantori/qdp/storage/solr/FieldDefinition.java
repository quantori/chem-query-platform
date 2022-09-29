package com.quantori.qdp.storage.solr;

import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
class FieldDefinition {
  String name;
  String type;
  boolean stored;
  boolean indexed;
  boolean multiValued;
}
