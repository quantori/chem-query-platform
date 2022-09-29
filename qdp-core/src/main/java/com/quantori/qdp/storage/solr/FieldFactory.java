package com.quantori.qdp.storage.solr;

import java.util.List;
import lombok.experimental.UtilityClass;

@UtilityClass
class FieldFactory {

  static final List<FieldDefinition> moleculeFields = List.of(
      new FieldDefinition("id", "string", true, true, false),
      new FieldDefinition("libraryId", "text_general", true, true, false),
      new FieldDefinition("structure", "binary", true, true, false),
      new FieldDefinition("sub", "text_general", true, true, false),
      new FieldDefinition("sim", "string", true, true, false),
      new FieldDefinition("exact", "text_general", true, true, false),
      new FieldDefinition("index", "text_general", true, true, false),
      new FieldDefinition("_version_", "plong", true, true, false)
  );
  static final List<FieldDefinition> reactionFields = List.of(
      new FieldDefinition("id", "string", true, true, false),
      new FieldDefinition("reactionSmiles", "text_general", true, false, false),
      new FieldDefinition("sub", "text_general", true, true, false),
      new FieldDefinition("reactionId", "text_general", true, true, false),
      new FieldDefinition("source", "text_general", true, false, false),
      new FieldDefinition("description", "text_general", true, false, false),
      new FieldDefinition("amount", "text_general", true, false, false),
      new FieldDefinition("_version_", "plong", true, true, false)
  );
  static final List<FieldDefinition> reactionParticipantsFields = List.of(
      new FieldDefinition("id", "string", true, true, false),
      new FieldDefinition("reactionId", "text_general", true, true, false),
      new FieldDefinition("libraryId", "text_general", true, true, false),
      new FieldDefinition("structure", "text_general", true, false, false),
      new FieldDefinition("name", "text_general", true, false, false),
      new FieldDefinition("inchi", "text_general", true, false, false),
      new FieldDefinition("sub", "text_general", true, true, false),
      new FieldDefinition("exact", "text_general", true, true, false),
      new FieldDefinition("role", "text_general", true, true, false),
      new FieldDefinition("type", "text_general", true, true, false),
      new FieldDefinition("_version_", "plong", true, true, false)
  );
  static final List<FieldDefinition> libraryFields = List.of(
      new FieldDefinition("id", "string", true, true, false),
      new FieldDefinition("structures_count", "text_general", true, true, false),
      new FieldDefinition("name", "text_general", true, true, false),
      new FieldDefinition("type", "string", true, true, false),
      new FieldDefinition("created_timestamp", "text_general", true, true, false),
      new FieldDefinition("updated_timestamp", "text_general", true, true, false),
      new FieldDefinition("user_data", "text_general", true, true, false),
      new FieldDefinition("properties", "text_general", true, true, true),
      new FieldDefinition("_version_", "plong", true, true, false)
  );
}
