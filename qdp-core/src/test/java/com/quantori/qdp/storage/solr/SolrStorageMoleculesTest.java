package com.quantori.qdp.storage.solr;

import static com.quantori.qdp.storage.solr.TestIndigoFingerPrintUtilities.getExactBinaryFingerprint;
import static com.quantori.qdp.storage.solr.TestIndigoFingerPrintUtilities.getSubstructureBinaryFingerprint;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.epam.indigo.Indigo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quantori.qdp.storage.api.Flattened;
import com.quantori.qdp.storage.api.Library;
import com.quantori.qdp.storage.api.LibraryType;
import com.quantori.qdp.storage.api.Molecule;
import com.quantori.qdp.storage.api.MoleculesWriter;
import com.quantori.qdp.storage.api.ReactionParticipantRole;
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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class SolrStorageMoleculesTest {

  private static final String LIBRARY_NAME = "molecules";
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final Indigo indigo = new Indigo();
  private static SolrStorageLibrary storageLibrary;
  private static SolrStorageMolecules storageMolecules;

  @BeforeAll
  void setUp() {
    SolrProperties props = new SolrProperties();
    props.setUrl(SolrTestContainer.startContainer());
    SolrStorageConfiguration config = new SolrStorageConfiguration(props);
    storageLibrary = new SolrStorageLibrary(props.getLibrariesCollectionName(), props.isRollbackEnabled(),
        config.getSolrClient(), config.getSolrUpdateClient());
    storageMolecules = new SolrStorageMolecules(props.isRollbackEnabled(),
        config.getSolrClient(), config.getSolrUpdateClient(), storageLibrary);
  }

  @Test
  @Order(1)
  void createMoleculeLibrary() {
    Library library = storageLibrary.createLibrary(LIBRARY_NAME, LibraryType.molecules, new SolrServiceData());
    assertEquals(LIBRARY_NAME, library.getName());
  }

  @Test
  @Order(2)
  void populateMoleculeLibrary() throws IOException {
    String moleculeLibraryName = "/storage/solr/test_library_qa_full.json";
    List<Molecule> moleculesToUpload = objectMapper.readValue(this.getClass()
        .getResourceAsStream(moleculeLibraryName), new TypeReference<>() {
    });
    MoleculesWriter moleculesWriter = storageMolecules
        .buildMoleculeWriter(storageLibrary.getLibraryByName(LIBRARY_NAME).iterator().next());
    moleculesToUpload.forEach(moleculesWriter::write);
    moleculesWriter.close();
    assertEquals(15, storageLibrary.getLibraryByName(LIBRARY_NAME).iterator().next().getStructuresCount());
  }

  @Test
  @Order(3)
  void searchExact() {
    Library library = storageLibrary.getLibraryByName(LIBRARY_NAME).iterator().next();
    byte[] hash = getExactBinaryFingerprint("C1=C(C(=CC(=C1N)N)N)N", List.of());
    SearchIterator<Flattened.Molecule> iterator = storageMolecules
        .searchExact(new String[] {library.getId()}, hash, List.of(), ReactionParticipantRole.none);
    List<Flattened.Molecule> molecules = iterator.next();
    assertTrue(iterator.next().isEmpty());
    assertEquals(1, molecules.size());
    byte[] resultHash = getExactBinaryFingerprint(indigo.deserialize(
        Base64.getDecoder().decode(molecules.iterator().next().getEncodedStructure())).smiles(), List.of());
    assertArrayEquals(hash, resultHash);
  }

  @Test
  @Order(3)
  void searchSub() {
    Library library = storageLibrary.getLibraryByName(LIBRARY_NAME).iterator().next();
    byte[] hash = getSubstructureBinaryFingerprint("C1CCCCC1", List.of());
    SearchIterator<Flattened.Molecule> iterator = storageMolecules
        .searchSub(new String[] {library.getId()}, hash, List.of(), ReactionParticipantRole.none);

    List<Flattened.Molecule> molecules = iterator.next();
    assertTrue(iterator.next().isEmpty());
    assertEquals(2, molecules.size());
    List<Integer> intHash = BitSet.valueOf(hash).stream().boxed().toList();
    for (Flattened.Molecule molecule : molecules) {
      byte[] resultHash = getSubstructureBinaryFingerprint(indigo.deserialize(
          Base64.getDecoder().decode(molecule.getEncodedStructure())).smiles(), List.of());
      List<Integer> resultIntHash = BitSet.valueOf(resultHash).stream().boxed().toList();
      assertTrue(resultIntHash.containsAll(intHash));
    }
  }

  @Test
  @Order(3)
  void searchSimTanimoto() {
    assertTrue(true);
  }
}
