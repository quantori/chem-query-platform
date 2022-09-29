package com.quantori.qdp.storage.solr;

import com.quantori.qdp.storage.api.FingerPrintUtilities;
import com.quantori.qdp.storage.api.Flattened;
import com.quantori.qdp.storage.api.Library;
import com.quantori.qdp.storage.api.LibraryType;
import com.quantori.qdp.storage.api.ReactionParticipantRole;
import com.quantori.qdp.storage.api.StorageType;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.common.SolrDocument;

@Slf4j
@UtilityClass
class Mapper {

  private static final List<String> moleculeStaticFields = FieldFactory.moleculeFields.stream()
      .map(FieldDefinition::name)
      .toList();

  static Flattened.Reaction flattenReaction(SolrDocument solrDocument, String libraryId) {
    var flattenedReaction = new Flattened.Reaction();
    flattenedReaction.setStructure(solrDocument.getOrDefault("reactionSmiles", "").toString());
    flattenedReaction.setReactionId(solrDocument.getOrDefault("reactionId", "").toString());
    flattenedReaction.setId(solrDocument.getOrDefault("id", "").toString());
    flattenedReaction.setAmount(solrDocument.getOrDefault("amount", "").toString());
    flattenedReaction.setSource(solrDocument.getOrDefault("source", "").toString());
    flattenedReaction.setDescription(solrDocument.getOrDefault("description", "").toString());
    flattenedReaction.setLibraryId(libraryId);
    flattenedReaction.setStorageType(StorageType.solr);
    return flattenedReaction;
  }

  static Flattened.Molecule flattenMolecule(SolrDocument solrDocument, String libraryId, String libraryName) {
    var molecule = new Flattened.Molecule();
    molecule.setId(solrDocument.getOrDefault("id", "").toString());
    molecule.setEncodedStructure(new String(Base64.getEncoder().encode((byte[])
        solrDocument.getOrDefault("structure", new byte[] {})), StandardCharsets.UTF_8));
    molecule.setLibraryId(libraryId);
    molecule.setLibraryName(libraryName);
    molecule.setProperties(solrDocument.entrySet().stream()
        .filter(entry -> !moleculeStaticFields.contains(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, entry -> formatProperty(selfOrFirstItem(entry.getValue())))));
    molecule.setRole(ReactionParticipantRole.none);
    molecule.setStorageType(StorageType.solr);
    return molecule;
  }

  static Flattened.Molecule flattenReactionParticipant(SolrDocument solrDocument, String libraryId,
                                                       String libraryName) {
    var flattenedMolecule = new Flattened.Molecule();
    flattenedMolecule.setId(solrDocument.getOrDefault("id", "").toString());
    flattenedMolecule.setEncodedStructure(solrDocument.getOrDefault("structure", "").toString());
    flattenedMolecule.setLibraryId(libraryId);
    flattenedMolecule.setLibraryName(libraryName);
    flattenedMolecule.setReactionId(solrDocument.getOrDefault("reactionId", "").toString());
    flattenedMolecule.setRole(ReactionParticipantRole.valueOf(solrDocument.getOrDefault("role", "").toString()));
    flattenedMolecule.setStorageType(StorageType.solr);
    return flattenedMolecule;
  }

  static Flattened.ReactionParticipant flattenReactionParticipant(SolrDocument solrDocument) {
    var reactionParticipant = new Flattened.ReactionParticipant();
    String structure = solrDocument.getOrDefault("structure", "").toString();
    if (!structure.isBlank()) {
      reactionParticipant.setSmiles(FingerPrintUtilities.decodeStructureIntoSmiles(structure, false));
    }
    reactionParticipant.setInchi(solrDocument.getOrDefault("inchi", "").toString());
    reactionParticipant.setRole(ReactionParticipantRole.valueOf(solrDocument.getOrDefault("role", "").toString()));
    reactionParticipant.setName(solrDocument.getOrDefault("name", "").toString());
    reactionParticipant.setType(solrDocument.getOrDefault("type", "").toString());
    return reactionParticipant;
  }

  static Library toLibrary(SolrDocument solrDocument) {
    var library = new Library();
    library.setId(solrDocument.getOrDefault("id", "").toString());
    library.setName(solrDocument.getOrDefault("name", "").toString());
    library.setCreatedStamp(from(solrDocument, "created_timestamp"));
    library.getServiceData().put(SolrServiceData.PROPERTIES, toProperties(solrDocument));
    library.setUpdatedStamp(from(solrDocument, "updated_timestamp"));
    library.setStructuresCount(Long.parseLong(solrDocument.getOrDefault("structures_count", 0).toString()));
    library.setType(LibraryType.valueOf(solrDocument.get("type").toString()));
    return library;
  }

  private static ZonedDateTime from(final SolrDocument solrDocument, final String fieldName) {
    final var value = solrDocument.get(fieldName);
    if (value == null) {
      return null;
    }

    String pref = "java.time.ZonedDateTime:";
    String suff = String.valueOf(value).substring(pref.length());

    if (value instanceof String) {
      return ZonedDateTime.parse(suff);
    }
    //this should not happen
    throw new RuntimeException("Field " + fieldName + " expected to be string but was " + value);
  }

  private static String selfOrFirstItem(Object value) {
    return (value instanceof ArrayList list) ? list.get(0).toString() : value.toString();
  }

  @SuppressWarnings({"unchecked"})
  private static List<String> toProperties(final SolrDocument document) {
    return (List<String>) document.getOrDefault("properties", List.of());
  }

  private static String formatProperty(String value) {
    try {
      return new BigDecimal(value).setScale(0, RoundingMode.UNNECESSARY).toString();
    } catch (NumberFormatException | ArithmeticException e) {
      return value;
    }
  }
}
