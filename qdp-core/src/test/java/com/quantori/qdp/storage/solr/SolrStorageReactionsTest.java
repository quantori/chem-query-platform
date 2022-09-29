package com.quantori.qdp.storage.solr;

import static com.quantori.qdp.storage.solr.TestIndigoFingerPrintUtilities.getExactBinaryFingerprint;
import static com.quantori.qdp.storage.solr.TestIndigoFingerPrintUtilities.getSubstructureBinaryFingerprint;
import static com.quantori.qdp.storage.solr.TestIndigoFingerPrintUtilities.getSubstructureBinaryFingerprintOfReaction;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.epam.indigo.Indigo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quantori.qdp.storage.api.Flattened;
import com.quantori.qdp.storage.api.Library;
import com.quantori.qdp.storage.api.LibraryType;
import com.quantori.qdp.storage.api.ReactionParticipantRole;
import com.quantori.qdp.storage.api.ReactionUploadDocument;
import com.quantori.qdp.storage.api.ReactionsWriter;
import com.quantori.qdp.storage.api.SearchIterator;
import java.io.IOException;
import java.util.Base64;
import java.util.BitSet;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.opentest4j.AssertionFailedError;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SolrStorageReactionsTest {

  private static final String LIBRARY_NAME = "reactions";
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Indigo indigo = new Indigo();
  private static SolrStorageLibrary storageLibrary;
  private static SolrStorageMolecules storageMolecules;
  private static SolrStorageReactions storageReactions;

  @BeforeAll
  void setUp() {
    SolrProperties props = new SolrProperties();
    props.setUrl(SolrTestContainer.startContainer());
    SolrStorageConfiguration config = new SolrStorageConfiguration(props);
    storageLibrary = new SolrStorageLibrary(props.getLibrariesCollectionName(), props.isRollbackEnabled(),
        config.getSolrClient(), config.getSolrUpdateClient());
    storageMolecules = new SolrStorageMolecules(props.isRollbackEnabled(),
        config.getSolrClient(), config.getSolrUpdateClient(), storageLibrary);
    storageReactions = new SolrStorageReactions(props.isRollbackEnabled(),
        config.getSolrClient(), config.getSolrUpdateClient(), storageLibrary);
  }

  @Test
  @Order(1)
  void createReactionLibrary() {
    Library library = storageLibrary.createLibrary(LIBRARY_NAME, LibraryType.reactions, new SolrServiceData());
    assertEquals(LIBRARY_NAME, library.getName());
  }

  @Test
  @Order(2)
  void populateReactionLibrary() throws IOException {
    String reactionLibraryName = "/storage/solr/Testlib_reaction_amount_test.json";
    List<ReactionUploadDocument> reactionsToUpload = objectMapper.readValue(this.getClass()
        .getResourceAsStream(reactionLibraryName), new TypeReference<>() {
    });
    ReactionsWriter reactionsWriter = storageReactions
        .buildReactionWriter(storageLibrary.getLibraryByName(LIBRARY_NAME).iterator().next());
    reactionsToUpload.forEach(reactionsWriter::write);
    reactionsWriter.close();
    assertEquals(4, storageLibrary.getLibraryByName(LIBRARY_NAME).iterator().next().getStructuresCount());
  }

  @Test
  @Order(3)
  void searchReaction() {
    Library library = storageLibrary.getLibraryByName(LIBRARY_NAME).iterator().next();
    byte[] hash = getSubstructureBinaryFingerprintOfReaction("$RXN\n\n\n\n  0  0  0\n", List.of());
    SearchIterator<Flattened.Reaction> iterator = storageReactions
        .searchReactions(new String[] {library.getId()}, hash);

    List<Flattened.Reaction> reactions = iterator.next();
    assertTrue(iterator.next().isEmpty());
    assertEquals(4, reactions.size());
    List<Integer> intHash = BitSet.valueOf(hash).stream().boxed().toList();
    for (Flattened.Reaction reaction : reactions) {
      byte[] resultHash = getSubstructureBinaryFingerprintOfReaction(reaction.getStructure(), List.of());
      List<Integer> resultIntHash = BitSet.valueOf(resultHash).stream().boxed().toList();
      assertTrue(resultIntHash.containsAll(intHash));
      Flattened.Reaction byIdReaction = storageReactions.searchById(library, reaction.getId())
          .orElseThrow(AssertionFailedError::new);
      assertEquals(reaction.getReactionId(), byIdReaction.getReactionId());
      assertEquals(reaction.getId(), byIdReaction.getId());
      List<Flattened.ReactionParticipant> participants = storageReactions
          .searchParticipantsByReactionId(library, reaction.getId());
      assertFalse(participants.isEmpty());
      participants.forEach(participant -> assertNotNull(participant.getRole()));
    }
  }

  @Test
  @Order(3)
  void searchExactParticipants() {
    Library library = storageLibrary.getLibraryByName(LIBRARY_NAME).iterator().next();
    byte[] hash = getExactBinaryFingerprint("CCCCCC", List.of());
    SearchIterator<Flattened.Molecule> iterator = storageMolecules
        .searchExact(new String[] {library.getId()}, hash, List.of(), ReactionParticipantRole.spectator);
    List<Flattened.Molecule> molecules = iterator.next();
    assertTrue(iterator.next().isEmpty());
    assertEquals(1, molecules.size());
    byte[] resultHash = getExactBinaryFingerprint(indigo.deserialize(
        Base64.getDecoder().decode(molecules.iterator().next().getEncodedStructure())).smiles(), List.of());
    assertArrayEquals(hash, resultHash);
  }

  @Test
  @Order(3)
  void searchSubParticipants() {
    Library library = storageLibrary.getLibraryByName(LIBRARY_NAME).iterator().next();
    byte[] hash = getSubstructureBinaryFingerprint("CCOCC", List.of());
    SearchIterator<Flattened.Molecule> iterator = storageMolecules
        .searchSub(new String[] {library.getId()}, hash, List.of(), ReactionParticipantRole.reactant);

    List<Flattened.Molecule> molecules = iterator.next();
    assertTrue(iterator.next().isEmpty());
    assertEquals(5, molecules.size());
    List<Integer> intHash = BitSet.valueOf(hash).stream().boxed().toList();
    for (Flattened.Molecule molecule : molecules) {
      byte[] resultHash = getSubstructureBinaryFingerprint(indigo.deserialize(
          Base64.getDecoder().decode(molecule.getEncodedStructure())).smiles(), List.of());
      List<Integer> resultIntHash = BitSet.valueOf(resultHash).stream().boxed().toList();
      assertTrue(resultIntHash.containsAll(intHash));
    }
  }

}
