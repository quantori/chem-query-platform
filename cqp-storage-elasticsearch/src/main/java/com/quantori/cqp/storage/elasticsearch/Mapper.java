package com.quantori.cqp.storage.elasticsearch;

import com.quantori.cqp.api.model.Flattened;
import com.quantori.cqp.api.model.Library;
import com.quantori.cqp.api.model.LibraryType;
import com.quantori.cqp.api.model.Property;
import com.quantori.cqp.api.model.upload.BasicMolecule;
import com.quantori.cqp.api.model.upload.Molecule;
import com.quantori.cqp.api.model.upload.Reaction;
import com.quantori.cqp.api.model.upload.ReactionParticipant;
import com.quantori.cqp.api.service.BaseFingerprintCalculator;
import com.quantori.cqp.api.service.MoleculesFingerprintCalculator;
import com.quantori.cqp.api.service.ReactionsFingerprintCalculator;
import com.quantori.cqp.api.util.FingerPrintUtilities;
import com.quantori.cqp.storage.elasticsearch.model.BasicMoleculeDocument;
import com.quantori.cqp.storage.elasticsearch.model.LibraryDocument;
import com.quantori.cqp.storage.elasticsearch.model.MoleculeDocument;
import com.quantori.cqp.storage.elasticsearch.model.ReactionDocument;
import com.quantori.cqp.storage.elasticsearch.model.ReactionParticipantDocument;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@UtilityClass
public class Mapper {

  static LibraryDocument toLibraryDocument(String libraryName, LibraryType libraryType,
                                           Map<String, Property> propertiesMapping) {
    var libraryDocument = new LibraryDocument(libraryName, libraryType, propertiesMapping);
    var now = Mapper.toTimestamp(Mapper.toInstant());
    libraryDocument.setCreatedStamp(now);
    libraryDocument.setUpdatedStamp(now);
    return libraryDocument;
  }

  static void addPropertiesMappingToLibraryDocument(LibraryDocument libraryDocument,
                                                               Map<String, Property> propertiesMapping) {
    Map<String, Property> allPropertiesMapping = new LinkedHashMap<>();
    Optional.of(libraryDocument.getPropertiesMapping()).ifPresent(allPropertiesMapping::putAll);
    Optional.of(propertiesMapping).ifPresent(allPropertiesMapping::putAll);
    libraryDocument.setPropertiesMapping(allPropertiesMapping);
  }

  static Library toLibrary(String id, LibraryDocument libraryDocument) {
    var library = new Library();
    library.setId(id);
    library.setName(Objects.requireNonNullElse(libraryDocument.getName(), ""));
    library.setCreatedStamp(Mapper.toInstant(libraryDocument.getCreatedStamp()));
    library.setUpdatedStamp(Mapper.toInstant(libraryDocument.getUpdatedStamp()));
    library.setStructuresCount(libraryDocument.getSize());
    library.setType(LibraryType.valueOf(libraryDocument.getType()));
    return library;
  }

  static Flattened.Molecule flattenMolecule(String id, String index, MoleculeDocument moleculeDocument,
                                            List<Library> libraries) {
    var molecule = new Flattened.Molecule();
    molecule.setId(id);
    molecule.setLibraryId(index);
    molecule.setLibraryName(
      libraries.stream()
        .filter(library -> Objects.equals(library.getId(), index))
        .map(Library::getName)
        .findFirst()
        .orElse(null)
    );
    molecule.setStructure(FingerPrintUtilities
      .decodeStructure(Objects.requireNonNullElse(moleculeDocument.getStructure(), "")));
    molecule.setSmiles(moleculeDocument.getSmiles());
    molecule.setExactHash(moleculeDocument.getExactHash());
    Map<String, String> molProperties = moleculeDocument.getMolProperties().entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, entry -> formatProperty(entry.getValue().toString())));
    molecule.setMolProperties(molProperties);
    molecule.setStorageType(ElasticsearchStorageConfiguration.STORAGE_TYPE);
    molecule.setCustomOrder(moleculeDocument.getCustomOrder());
    molecule.setCreatedStamp(Mapper.toInstant(moleculeDocument.getCreatedStamp()));
    molecule.setUpdatedStamp(Mapper.toInstant(moleculeDocument.getUpdatedStamp()));
    return molecule;
  }

  static Flattened.Reaction flattenReaction(String id, String index, ReactionDocument reactionDocument,
                                            List<Library> libraries) {
    var reaction = new Flattened.Reaction();
    reaction.setId(id);
    reaction.setLibraryId(index);
    reaction.setLibraryName(
      libraries.stream()
        .filter(library -> Objects.equals(library.getId(), index))
        .map(Library::getName)
        .findFirst()
        .orElse(null)
    );
    reaction.setReactionDocumentId(reactionDocument.getReactionDocumentId());
    reaction.setReactionSmiles(Objects.requireNonNullElse(reactionDocument.getReactionSmiles(), ""));
    reaction.setAmount(Objects.requireNonNullElse(reactionDocument.getAmount(), ""));
    reaction.setParagraphText(Objects.requireNonNullElse(reactionDocument.getParagraphText(), ""));
    reaction.setSource(Objects.requireNonNullElse(reactionDocument.getSource(), ""));
    reaction.setStorageType(ElasticsearchStorageConfiguration.STORAGE_TYPE);
    reaction.setCreatedStamp(toInstant(reactionDocument.getCreatedStamp()));
    reaction.setUpdatedStamp(toInstant(reactionDocument.getUpdatedStamp()));
    return reaction;
  }

  static Flattened.ReactionParticipant flattenReactionParticipant(String id,
                                                                  ReactionParticipantDocument reactionParticipantDocument) {
    var reactionParticipant = new Flattened.ReactionParticipant();
    reactionParticipant.setId(id);
    reactionParticipant.setType(Objects.requireNonNullElse(reactionParticipantDocument.getType(), ""));
    reactionParticipant.setRole(reactionParticipantDocument.getReactionParticipantRole());
    reactionParticipant.setName(Objects.requireNonNullElse(reactionParticipantDocument.getName(), ""));
    reactionParticipant.setSmiles(Objects.requireNonNullElse(reactionParticipantDocument.getSmiles(), ""));
    reactionParticipant.setInchi(Objects.requireNonNullElse(reactionParticipantDocument.getInchi(), ""));
    return reactionParticipant;
  }

  static MoleculeDocument convertToDocument(Molecule molecule, MoleculesFingerprintCalculator fingerprintCalculator) {
    MoleculeDocument moleculeDocument = new MoleculeDocument();
    copyToDocument(molecule, moleculeDocument, fingerprintCalculator);
    byte[] sim = Optional.ofNullable(molecule.getSim())
      .orElseGet(() -> fingerprintCalculator.similarityFingerprint(molecule.getStructure()));
    moleculeDocument.setSimHash(ElasticsearchFingerprintUtilities.getSimilarityHash(sim));
    Map<String, Object> molProperties = molecule.getMolProperties().entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, entry -> convert(entry.getValue())));
    moleculeDocument.setMolProperties(molProperties);
    moleculeDocument.setCustomOrder(molecule.getCustomOrder());
    Instant now = Mapper.toInstant();
    moleculeDocument.setCreatedStamp(toTimestamp(Objects.requireNonNullElse(molecule.getCreatedStamp(), now)));
    moleculeDocument.setUpdatedStamp(toTimestamp(Objects.requireNonNullElse(molecule.getUpdatedStamp(), now)));
    return moleculeDocument;
  }

  static Optional<String> elasticMoleculeFieldName(String fieldName) {
    if (StringUtils.isBlank(fieldName)) {
      return Optional.empty();
    }

    return Stream.of(
        MoleculeDocument.FieldMapping.CUSTOM_ORDER,
        MoleculeDocument.FieldMapping.CREATED_TIMESTAMP,
        MoleculeDocument.FieldMapping.UPDATED_TIMESTAMP,
        MoleculeDocument.FieldMapping.EXACT
      )
      .filter(fieldMapping -> StringUtils.equalsIgnoreCase(fieldMapping.getPropertyName(), fieldName))
      .findFirst()
      .map(MoleculeDocument.FieldMapping::getElasticName);
  }

  private static Object convert(String value) {
    if (value == null) {
      return "";
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      try {
        return Double.parseDouble(value);
      } catch (NumberFormatException ex) {
        return value;
      }
    }
  }

  static ReactionDocument convertToDocument(Reaction reaction, ReactionsFingerprintCalculator fingerprintCalculator) {
    ReactionDocument reactionDocument = new ReactionDocument();
    reactionDocument.setReactionSmiles(reaction.getReactionSmiles());
    reactionDocument.setReactionDocumentId(reaction.getReactionDocumentId());
    reactionDocument.setSource(reaction.getSource());
    reactionDocument.setParagraphText(reaction.getParagraphText());
    reactionDocument.setAmount(reaction.getAmount());
    byte[] sub = Optional.ofNullable(reaction.getSub())
      .orElseGet(() -> fingerprintCalculator.substructureFingerprint(reaction.getReactionSmiles()));
    reactionDocument.setSubHash(FingerPrintUtilities.substructureHash(sub));
    Instant now = Mapper.toInstant();
    reactionDocument.setCreatedStamp(toTimestamp(Objects.requireNonNullElse(reaction.getCreatedStamp(), now)));
    reactionDocument.setUpdatedStamp(toTimestamp(Objects.requireNonNullElse(reaction.getUpdatedStamp(), now)));
    return reactionDocument;
  }

  static ReactionParticipantDocument convertToDocument(ReactionParticipant reactionParticipant,
                                                       ReactionsFingerprintCalculator fingerprintCalculator) {
    ReactionParticipantDocument reactionParticipantDocument = new ReactionParticipantDocument();
    copyToDocument(reactionParticipant, reactionParticipantDocument, fingerprintCalculator);
    reactionParticipantDocument.setName(reactionParticipant.getName());
    reactionParticipantDocument.setInchi(reactionParticipant.getInchi());
    reactionParticipantDocument.setReactionId(reactionParticipant.getReactionId());
    reactionParticipantDocument.setReactionParticipantRole(reactionParticipant.getRole());
    reactionParticipantDocument.setType(reactionParticipant.getType());
    return reactionParticipantDocument;
  }

  private static void copyToDocument(BasicMolecule molecule, BasicMoleculeDocument moleculeDocument,
                                     BaseFingerprintCalculator fingerprintCalculator) {
    moleculeDocument.setSmiles(molecule.getSmiles());
    byte[] structure = molecule.getStructure();
    moleculeDocument.setStructure(structure == null ? null : FingerPrintUtilities.encodeStructure(structure));
    byte[] exact = Optional.ofNullable(molecule.getExact())
      .orElseGet(() -> fingerprintCalculator.exactFingerprint(structure));
    moleculeDocument.setExactHash(exact == null ? null : FingerPrintUtilities.exactHash(exact));
    byte[] sub = Optional.ofNullable(molecule.getSub())
      .orElseGet(() -> fingerprintCalculator.substructureFingerprint(structure));
    moleculeDocument.setSubHash(sub == null ? null : FingerPrintUtilities.substructureHash(sub));
  }

  private static String formatProperty(String value) {
    try {
      return new BigDecimal(value).toPlainString();
    } catch (NumberFormatException | ArithmeticException e) {
      return value;
    }
  }

  public static Instant toInstant() {
    return Instant.now().truncatedTo(ChronoUnit.MILLIS);
  }

  private static Instant toInstant(Long timestamp) {
    if (timestamp == null) {
      return null;
    } else {
      return Instant.ofEpochMilli(timestamp);
    }
  }

  public static Long toTimestamp(Instant date) {
    if (date == null) {
      return null;
    } else {
      return date.toEpochMilli();
    }
  }
}
