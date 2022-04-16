package com.quantori.qdp.core.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.times;

import com.quantori.qdp.core.source.model.DataLibrary;
import com.quantori.qdp.core.source.model.DataLibraryType;
import com.quantori.qdp.core.source.model.DataLoader;
import com.quantori.qdp.core.source.model.DataSource;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.TransformationStep;
import com.quantori.qdp.core.source.model.TransformationStepBuilder;
import com.quantori.qdp.core.source.model.molecule.Molecule;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class MoleculeServiceTest {
  private static final String TEST_STORAGE = "test_storage";
  private static final String TEST_STORAGE_2 = "test_storage-2";
  private static final String LIBRARY_NAME = "qdp_mol_service_name";
  private static final int MAX_UPLOADS = 3;

  @SuppressWarnings("unchecked")
  @Test
  void registerMoleculeStorage() throws ExecutionException, InterruptedException {
    MoleculeService service = new MoleculeService();

    var storage = Mockito.mock(DataStorage.class);
    service.registerMoleculeStorage(storage, TEST_STORAGE, MAX_UPLOADS);
    var storage2 = Mockito.mock(DataStorage.class);
    service.registerMoleculeStorage(storage2, TEST_STORAGE_2, MAX_UPLOADS);

    var listOfSources = service.listSources().toCompletableFuture().get();
    assertEquals(2, listOfSources.size());

    assertEquals(1, listOfSources.stream().filter(s -> s.storageName.equals(TEST_STORAGE)).count());
    assertEquals(1, listOfSources.stream().filter(s -> s.storageName.equals(TEST_STORAGE_2)).count());
  }

  @SuppressWarnings("unchecked")
  @Test
  void loadMoleculesFromDataSource() throws Exception {
    var storage = Mockito.mock(DataStorage.class);
    var loader = Mockito.mock(DataLoader.class);
    Mockito.doNothing().when(loader).add(Mockito.any());
    Mockito.when(storage.dataLoader(Mockito.any())).thenReturn(loader);

    DataSource<Molecule> source = (DataSource<Molecule>) Mockito.mock(DataSource.class);
    Mockito.when(source.createIterator()).thenReturn(List.of(new Molecule()).iterator());

    Function<Molecule, Molecule> func = (qdpMolecule) -> {
      qdpMolecule.setId("transformed");
      return qdpMolecule;
    };

    TransformationStep<Molecule, Molecule> step = TransformationStepBuilder.builder(func).build();

    MoleculeService service = new MoleculeService();
    service.registerMoleculeStorage(storage, TEST_STORAGE, MAX_UPLOADS);

    var stat = service.loadMoleculesFromDataSource(TEST_STORAGE, LIBRARY_NAME, source, step)
        .toCompletableFuture().get();
    assertFalse(stat.isFailed());
    assertEquals(1, stat.getCountOfSuccessfullyProcessed());

    ArgumentCaptor<Molecule> qdpMoleculeCaptor = ArgumentCaptor.forClass(Molecule.class);
    Mockito.verify(loader).add(qdpMoleculeCaptor.capture());
    assertEquals("transformed", qdpMoleculeCaptor.getValue().getId());

    Mockito.verify(source, times(1)).createIterator();
    Mockito.verify(source, times(1)).close();
  }

  @SuppressWarnings("unchecked")
  @Test
  void getDataStorageIndexes() throws ExecutionException, InterruptedException {
    MoleculeService service = new MoleculeService();
    DataLibrary index = new DataLibrary(LIBRARY_NAME, DataLibraryType.MOLECULE, Map.of());
    var storage = Mockito.mock(DataStorage.class);
    Mockito.when(storage.getLibraries()).thenReturn(List.of(index));

    service.registerMoleculeStorage(storage, TEST_STORAGE, MAX_UPLOADS);
    service.createDataStorageIndex(TEST_STORAGE, index);
    var result = service.getDataStorageIndexes(TEST_STORAGE).toCompletableFuture().get();

    ArgumentCaptor<DataLibrary> valueCapture = ArgumentCaptor.forClass(DataLibrary.class);
    Mockito.verify(storage).createLibrary(valueCapture.capture());
    assertEquals(LIBRARY_NAME, valueCapture.getValue().getName());
    assertEquals(1, result.size());
    assertEquals(LIBRARY_NAME, result.get(0).getName());
  }
}