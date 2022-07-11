package com.quantori.qdp.core.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import com.quantori.qdp.core.source.model.DataSource;
import com.quantori.qdp.core.source.model.StorageItem;
import com.quantori.qdp.core.source.model.TransformationStep;
import com.quantori.qdp.core.source.model.TransformationStepBuilder;
import com.quantori.qdp.core.source.model.UploadItem;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class LoaderTest {
  static final ActorTestKit testKit = ActorTestKit.create();

  @AfterAll
  public static void teardown() {
    testKit.shutdownTestKit();
  }

  @SuppressWarnings("unchecked")
  @Test
  void loadOneMolecule() throws Exception {
    Loader loader = new Loader(testKit.system());

    DataSource<UploadItem> source = (DataSource<UploadItem>) Mockito.mock(DataSource.class);
    when(source.createIterator()).thenReturn(Stream.of(new Molecule()).map(molecule -> (UploadItem) molecule).collect(
        Collectors.toList()).iterator());
    Function<UploadItem, StorageItem> func = (qdpMolecule) -> {
      ((Molecule) qdpMolecule).setId("transformed");
      return (StorageItem) qdpMolecule;
    };

    TransformationStep<UploadItem, StorageItem> step = TransformationStepBuilder.builder(func).build();

    Consumer<StorageItem> consumer = Mockito.mock(Consumer.class);

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(1, stat.getCountOfSuccessfullyProcessed());
    assertEquals(0, stat.getCountOfErrors());

    ArgumentCaptor<Molecule> qdpMoleculeCaptor = ArgumentCaptor.forClass(Molecule.class);
    Mockito.verify(consumer).accept(qdpMoleculeCaptor.capture());
    assertEquals("transformed", qdpMoleculeCaptor.getValue().getId());
  }

  @SuppressWarnings("unchecked")
  @Test
  void loadSeveralMolecules() throws Exception {
    Loader loader = new Loader(testKit.system());

    DataSource<UploadItem> source = (DataSource<UploadItem>) Mockito.mock(DataSource.class);
    when(source.createIterator()).thenReturn(Stream.of(new Molecule(), new Molecule(), new Molecule())
        .map(molecule -> (UploadItem) molecule).collect(Collectors.toList()).iterator());
    Function<UploadItem, StorageItem> func = (qdpMolecule) -> {
      ((Molecule) qdpMolecule).setId("transformed");
      return (StorageItem) qdpMolecule;
    };

    TransformationStep<UploadItem, StorageItem> step = TransformationStepBuilder.builder(func).build();

    Consumer<StorageItem> consumer = Mockito.mock(Consumer.class);

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(3, stat.getCountOfSuccessfullyProcessed());
    assertEquals(0, stat.getCountOfErrors());

    ArgumentCaptor<Molecule> qdpMoleculeCaptor = ArgumentCaptor.forClass(Molecule.class);
    Mockito.verify(consumer, times(3)).accept(qdpMoleculeCaptor.capture());
    qdpMoleculeCaptor.getAllValues().forEach(mol -> assertEquals("transformed", mol.getId()));
  }

  @SuppressWarnings("unchecked")
  @Test
  void loadSkipTransformationErrors() throws Exception {
    Loader loader = new Loader(testKit.system());

    DataSource<UploadItem> source = (DataSource<UploadItem>) Mockito.mock(DataSource.class);
    when(source.createIterator()).thenReturn(Stream.of(new Molecule(), new Molecule("error"), new Molecule())
        .map(molecule -> (UploadItem) molecule).collect(Collectors.toList()).iterator());
    Function<UploadItem, StorageItem> func = (qdpMolecule) -> {
      if ("error".equals(((Molecule) qdpMolecule).getId())) {
        throw new RuntimeException("test");
      }
      ((Molecule) qdpMolecule).setId("transformed");
      return (StorageItem) qdpMolecule;
    };

    TransformationStep<UploadItem, StorageItem> step = TransformationStepBuilder.builder(func).build();

    Consumer<StorageItem> consumer = Mockito.mock(Consumer.class);

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(2, stat.getCountOfSuccessfullyProcessed());
    assertEquals(1, stat.getCountOfErrors());

    ArgumentCaptor<Molecule> qdpMoleculeCaptor = ArgumentCaptor.forClass(Molecule.class);
    Mockito.verify(consumer, times(2)).accept(qdpMoleculeCaptor.capture());
    qdpMoleculeCaptor.getAllValues().forEach(mol -> assertEquals("transformed", mol.getId()));
  }

  @SuppressWarnings("unchecked")
  @Test
  void loadSkipConsumerErrors() throws Exception {
    Loader loader = new Loader(testKit.system());

    DataSource<UploadItem> source = (DataSource<UploadItem>) Mockito.mock(DataSource.class);
    when(source.createIterator()).thenReturn(Stream.of(new Molecule(), new Molecule("error"), new Molecule())
        .map(molecule -> (UploadItem) molecule).collect(Collectors.toList()).iterator());
    Function<UploadItem, StorageItem> func = (qdpMolecule) -> {
      if ("error".equals(((Molecule) qdpMolecule).getId())) {
        return (StorageItem) qdpMolecule;
      }
      ((Molecule) qdpMolecule).setId("transformed");
      return (StorageItem) qdpMolecule;
    };

    TransformationStep<UploadItem, StorageItem> step = TransformationStepBuilder.builder(func).build();

    Consumer<StorageItem> consumer = Mockito.mock(Consumer.class);
    doAnswer(invocation -> {
      Molecule molecule = invocation.getArgument(0);
      if ("error".equals(molecule.getId())) {
        throw new RuntimeException("test");
      }
      return null;
    }).when(consumer).accept(any(StorageItem.class));

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(2, stat.getCountOfSuccessfullyProcessed());
    assertEquals(1, stat.getCountOfErrors());

    Mockito.verify(consumer, times(3)).accept(any(Molecule.class));
  }

  @SuppressWarnings({"unchecked"})
  @Test
  void loadZeroMolecules() throws Exception {
    Loader loader = new Loader(testKit.system());

    DataSource<UploadItem> source = (DataSource<UploadItem>) Mockito.mock(DataSource.class);
    when(source.createIterator()).thenReturn(Collections.emptyIterator());
    Function<UploadItem, StorageItem> func = (qdpMolecule) -> {
      ((Molecule) qdpMolecule).setId("transformed");
      return (StorageItem) qdpMolecule;
    };

    TransformationStep<UploadItem, StorageItem> step = TransformationStepBuilder.builder(func).build();

    Consumer<StorageItem> consumer = Mockito.mock(Consumer.class);

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(0, stat.getCountOfSuccessfullyProcessed());
    assertEquals(0, stat.getCountOfErrors());

    ArgumentCaptor<Molecule> qdpMoleculeCaptor = ArgumentCaptor.forClass(Molecule.class);
    Mockito.verify(consumer, times(0)).accept(qdpMoleculeCaptor.capture());
  }

  @SuppressWarnings("unchecked")
  @Test
  void loadNulMolecules() {
    Loader loader = new Loader(testKit.system());

    DataSource<UploadItem> source = (DataSource<UploadItem>) Mockito.mock(DataSource.class);
    List<UploadItem> list = new ArrayList<>();
    list.add(new Molecule());
    list.add(null);
    list.add(new Molecule());
    when(source.createIterator()).thenReturn(list.iterator());
    Function<UploadItem, StorageItem> func = (qdpMolecule) -> (StorageItem) qdpMolecule;

    TransformationStep<UploadItem, StorageItem> step = TransformationStepBuilder.builder(func).build();

    Consumer<StorageItem> consumer = Mockito.mock(Consumer.class);

    var ex = assertThrows(ExecutionException.class,
        () -> loader.loadStorageItems(source, step, consumer).toCompletableFuture().get());

    assertEquals("Element must not be null, rule 2.13", ex.getCause().getMessage());
  }

  @SuppressWarnings("unchecked")
  @Test
  void loadNulInQDPTransformationStep() throws Exception {
    Loader loader = new Loader(testKit.system());

    DataSource<UploadItem> source = (DataSource<UploadItem>) Mockito.mock(DataSource.class);
    when(source.createIterator()).thenReturn(Stream.of(new Molecule(), new Molecule("error"), new Molecule())
        .map(molecule -> (UploadItem) molecule).collect(Collectors.toList()).iterator());
    Function<UploadItem, StorageItem> func = (qdpMolecule) -> {
      if ("error".equals(((Molecule) qdpMolecule).getId())) {
        return null;
      }
      ((Molecule) qdpMolecule).setId("transformed");
      return (StorageItem) qdpMolecule;
    };

    TransformationStep<UploadItem, StorageItem> step = TransformationStepBuilder.builder(func).build();

    Consumer<StorageItem> consumer = Mockito.mock(Consumer.class);

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(2, stat.getCountOfSuccessfullyProcessed());
    assertEquals(0, stat.getCountOfErrors());

    ArgumentCaptor<Molecule> qdpMoleculeCaptor = ArgumentCaptor.forClass(Molecule.class);
    Mockito.verify(consumer, times(2)).accept(qdpMoleculeCaptor.capture());
  }

  @SuppressWarnings("unchecked")
  @Test
  void loadWithExceptionInQDPTransformationStep() throws Exception {
    Loader loader = new Loader(testKit.system());

    DataSource<UploadItem> source = (DataSource<UploadItem>) Mockito.mock(DataSource.class);
    when(source.createIterator()).thenReturn(Stream.of(new Molecule(), new Molecule(), new Molecule())
        .map(molecule -> (UploadItem) molecule).collect(Collectors.toList()).iterator());
    Function<UploadItem, StorageItem> func = (qdpMolecule) -> {
      throw new RuntimeException("test");
    };

    TransformationStep<UploadItem, StorageItem> step = TransformationStepBuilder.builder(func).build();

    Consumer<StorageItem> consumer = Mockito.mock(Consumer.class);

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(0, stat.getCountOfSuccessfullyProcessed());
    assertEquals(3, stat.getCountOfErrors());
  }

  @SuppressWarnings({"unchecked"})
  @Test
  void loadMoleculesWithBadSource() {
    var moleculeLoader = new Loader(testKit.system());

    DataSource<UploadItem> source = (DataSource<UploadItem>) Mockito.mock(DataSource.class);
    when(source.createIterator()).thenThrow(new RuntimeException("testEx"));
    Function<UploadItem, StorageItem> func = (qdpMolecule) -> (StorageItem) qdpMolecule;

    TransformationStep<UploadItem, StorageItem> step = TransformationStepBuilder.builder(func).build();

    Consumer<StorageItem> consumer = Mockito.mock(Consumer.class);

    var ex = assertThrows(ExecutionException.class,
        () -> moleculeLoader.loadStorageItems(source, step, consumer).toCompletableFuture().get());

    MatcherAssert
        .assertThat(ex.getCause().getMessage(), CoreMatchers.startsWith("testEx"));
  }

  @SuppressWarnings("unchecked")
  @Test
  void loadMoleculesWithBadSourceIterator() {
    Loader loader = new Loader(testKit.system());

    DataSource<UploadItem> source = (DataSource<UploadItem>) Mockito.mock(DataSource.class);
    Iterator<UploadItem> iterator = Mockito.mock(Iterator.class);

    when(iterator.hasNext()).thenReturn(true);
    when(iterator.next()).thenThrow(new RuntimeException("testEx"));
    when(source.createIterator()).thenReturn(iterator);
    Function<UploadItem, StorageItem> func = qdpMolecule -> (StorageItem) qdpMolecule;

    TransformationStep<UploadItem, StorageItem> step = TransformationStepBuilder.builder(func).build();

    Consumer<StorageItem> consumer = Mockito.mock(Consumer.class);

    var ex = assertThrows(ExecutionException.class,
        () -> loader.loadStorageItems(source, step, consumer).toCompletableFuture().get());

    assertEquals("testEx", ex.getCause().getMessage());
  }

  @SuppressWarnings("unchecked")
  @Test
  void loadMoleculesWithBadConsumer() throws Exception {
    Loader loader = new Loader(testKit.system());

    DataSource<UploadItem> source = (DataSource<UploadItem>) Mockito.mock(DataSource.class);
    when(source.createIterator()).thenReturn(Stream.of(new Molecule(), new Molecule(), new Molecule())
        .map(molecule -> (UploadItem) molecule).collect(Collectors.toList()).iterator());
    Function<UploadItem, StorageItem> func = (qdpMolecule) -> (StorageItem) qdpMolecule;

    TransformationStep<UploadItem, StorageItem> step = TransformationStepBuilder.builder(func).build();

    Consumer<StorageItem> consumer = Mockito.mock(Consumer.class);
    Mockito.doThrow(new RuntimeException()).when(consumer).accept(Mockito.any(Molecule.class));

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(0, stat.getCountOfSuccessfullyProcessed());
    assertEquals(3, stat.getCountOfErrors());
  }
}
