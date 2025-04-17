package com.quantori.cqp.storage.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.quantori.cqp.storage.elasticsearch.model.LibraryDocument;
import com.quantori.cqp.storage.elasticsearch.model.MoleculeDocument;
import com.quantori.cqp.storage.elasticsearch.model.ReactionDocument;
import com.quantori.cqp.storage.elasticsearch.model.ReactionParticipantDocument;
import lombok.experimental.UtilityClass;

@UtilityClass
class JsonMapper {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

  static String toJsonString(LibraryDocument libraryDocument) throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(libraryDocument);
  }

  static LibraryDocument toLibraryDocument(String json) throws JsonProcessingException {
    return OBJECT_MAPPER.readValue(json, LibraryDocument.class);
  }

  static String toJsonString(MoleculeDocument moleculeDocument) throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(moleculeDocument);
  }

  static MoleculeDocument toMoleculeDocument(String json) throws JsonProcessingException {
    return OBJECT_MAPPER.readValue(json, MoleculeDocument.class);
  }

  static String toJsonString(ReactionDocument reactionDocument) throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(reactionDocument);
  }

  static ReactionDocument toReactionDocument(String json) throws JsonProcessingException {
    return OBJECT_MAPPER.readValue(json, ReactionDocument.class);
  }

  static String toJsonString(ReactionParticipantDocument reactionParticipantDocument) throws JsonProcessingException {
    return OBJECT_MAPPER.writeValueAsString(reactionParticipantDocument);
  }

  static ReactionParticipantDocument toReactionParticipantDocument(String json) throws JsonProcessingException {
    return OBJECT_MAPPER.readValue(json, ReactionParticipantDocument.class);
  }

  static String objectToJSON(Object obj) {
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(obj);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return obj.toString();
  }
}
