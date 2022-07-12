package com.quantori.qdp.core.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import com.quantori.qdp.core.source.model.DataSource;
import com.quantori.qdp.core.source.model.TransformationStep;
import com.quantori.qdp.core.source.model.TransformationStepBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

class LoaderTest {
  static final ActorTestKit testKit = ActorTestKit.create();

  private Loader<Molecule, Molecule> loader;
  private DataSource<Molecule> source;
  private Consumer<Molecule> consumer;
  private Function<Molecule, Molecule> identity;

  @AfterAll
  public static void teardown() {
    testKit.shutdownTestKit();
  }

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() {
    loader = new Loader<>(testKit.system());
    source = (DataSource<Molecule>) Mockito.mock(DataSource.class);
    consumer = Mockito.mock(Consumer.class);
    identity = object -> object;
  }


  @Test
  void loadOneMolecule() throws Exception {
    when(source.createIterator()).thenReturn(List.of(new Molecule()).iterator());
    Function<Molecule, Molecule> func = (qdpMolecule) -> {
      qdpMolecule.setId("transformed");
      return qdpMolecule;
    };
    TransformationStep<Molecule, Molecule> step = TransformationStepBuilder.builder(func).build();

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(1, stat.getCountOfSuccessfullyProcessed());
    assertEquals(0, stat.getCountOfErrors());

    ArgumentCaptor<Molecule> qdpMoleculeCaptor = ArgumentCaptor.forClass(Molecule.class);
    Mockito.verify(consumer).accept(qdpMoleculeCaptor.capture());
    assertEquals("transformed", qdpMoleculeCaptor.getValue().getId());
  }

  @Test
  void loadSeveralMolecules() throws Exception {
    when(source.createIterator()).thenReturn(List.of(new Molecule(), new Molecule(), new Molecule()).iterator());
    Function<Molecule, Molecule> func = (qdpMolecule) -> {
      qdpMolecule.setId("transformed");
      return qdpMolecule;
    };
    TransformationStep<Molecule, Molecule> step = TransformationStepBuilder.builder(func).build();

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(3, stat.getCountOfSuccessfullyProcessed());
    assertEquals(0, stat.getCountOfErrors());

    ArgumentCaptor<Molecule> qdpMoleculeCaptor = ArgumentCaptor.forClass(Molecule.class);
    Mockito.verify(consumer, times(3)).accept(qdpMoleculeCaptor.capture());
    qdpMoleculeCaptor.getAllValues().forEach(mol -> assertEquals("transformed", mol.getId()));
  }

  @Test
  void loadSkipTransformationErrors() throws Exception {
    when(source.createIterator())
        .thenReturn(List.of(new Molecule(), new Molecule("error"), new Molecule()).iterator());
    Function<Molecule, Molecule> func = (qdpMolecule) -> {
      if ("error".equals(qdpMolecule.getId())) {
        throw new RuntimeException("test");
      }
      qdpMolecule.setId("transformed");
      return qdpMolecule;
    };
    TransformationStep<Molecule, Molecule> step = TransformationStepBuilder.builder(func).build();

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(2, stat.getCountOfSuccessfullyProcessed());
    assertEquals(1, stat.getCountOfErrors());

    ArgumentCaptor<Molecule> qdpMoleculeCaptor = ArgumentCaptor.forClass(Molecule.class);
    Mockito.verify(consumer, times(2)).accept(qdpMoleculeCaptor.capture());
    qdpMoleculeCaptor.getAllValues().forEach(mol -> assertEquals("transformed", mol.getId()));
  }

  @Test
  void loadSkipConsumerErrors() throws Exception {
    when(source.createIterator())
        .thenReturn(List.of(new Molecule(), new Molecule("error"), new Molecule()).iterator());
    Function<Molecule, Molecule> func = (qdpMolecule) -> {
      if ("error".equals(qdpMolecule.getId())) {
        return qdpMolecule;
      }
      qdpMolecule.setId("transformed");
      return qdpMolecule;
    };
    TransformationStep<Molecule, Molecule> step = TransformationStepBuilder.builder(func).build();
    doAnswer(invocation -> {
      Molecule molecule = invocation.getArgument(0);
      if ("error".equals(molecule.getId())) {
        throw new RuntimeException("test");
      }
      return null;
    }).when(consumer).accept(any(Molecule.class));

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(2, stat.getCountOfSuccessfullyProcessed());
    assertEquals(1, stat.getCountOfErrors());

    Mockito.verify(consumer, times(3)).accept(any(Molecule.class));
  }

  @Test
  void loadZeroMolecules() throws Exception {
    when(source.createIterator()).thenReturn(Collections.emptyIterator());
    TransformationStep<Molecule, Molecule> step = TransformationStepBuilder.builder(identity).build();

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(0, stat.getCountOfSuccessfullyProcessed());
    assertEquals(0, stat.getCountOfErrors());

    ArgumentCaptor<Molecule> qdpMoleculeCaptor = ArgumentCaptor.forClass(Molecule.class);
    Mockito.verify(consumer, times(0)).accept(qdpMoleculeCaptor.capture());
  }

  @Test
  void loadNulMolecules() {
    List<Molecule> list = new ArrayList<>();
    list.add(new Molecule());
    list.add(null);
    list.add(new Molecule());
    when(source.createIterator()).thenReturn(list.iterator());
    Function<Molecule, Molecule> func = qdpMolecule -> qdpMolecule;
    TransformationStep<Molecule, Molecule> step = TransformationStepBuilder.builder(func).build();

    var ex = assertThrows(ExecutionException.class,
        () -> loader.loadStorageItems(source, step, consumer).toCompletableFuture().get());

    assertEquals("Element must not be null, rule 2.13", ex.getCause().getMessage());
  }

  @Test
  void loadNulInQDPTransformationStep() throws Exception {
    when(source.createIterator()).thenReturn(List.of(new Molecule(), new Molecule("error"), new Molecule()).iterator());
    Function<Molecule, Molecule> func = (qdpMolecule) -> {
      if ("error".equals(qdpMolecule.getId())) {
        return null;
      }
      qdpMolecule.setId("transformed");
      return qdpMolecule;
    };
    TransformationStep<Molecule, Molecule> step = TransformationStepBuilder.builder(func).build();

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(2, stat.getCountOfSuccessfullyProcessed());
    assertEquals(0, stat.getCountOfErrors());

    ArgumentCaptor<Molecule> qdpMoleculeCaptor = ArgumentCaptor.forClass(Molecule.class);
    Mockito.verify(consumer, times(2)).accept(qdpMoleculeCaptor.capture());
  }

  @Test
  void loadWithExceptionInQDPTransformationStep() throws Exception {
    when(source.createIterator()).thenReturn(List.of(new Molecule(), new Molecule(), new Molecule()).iterator());
    Function<Molecule, Molecule> func = (qdpMolecule) -> {
      throw new RuntimeException("test");
    };
    TransformationStep<Molecule, Molecule> step = TransformationStepBuilder.builder(func).build();

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(0, stat.getCountOfSuccessfullyProcessed());
    assertEquals(3, stat.getCountOfErrors());
  }

  @Test
  void loadMoleculesWithBadSource() {
    when(source.createIterator()).thenThrow(new RuntimeException("testEx"));
    TransformationStep<Molecule, Molecule> step = TransformationStepBuilder.builder(identity).build();

    var ex = assertThrows(ExecutionException.class,
        () -> loader.loadStorageItems(source, step, consumer).toCompletableFuture().get());

    MatcherAssert.assertThat(ex.getCause().getMessage(), CoreMatchers.startsWith("testEx"));
  }

  @SuppressWarnings("unchecked")
  @Test
  void loadMoleculesWithBadSourceIterator() {
    Iterator<Molecule> iterator = Mockito.mock(Iterator.class);
    when(iterator.hasNext()).thenReturn(true);
    when(iterator.next()).thenThrow(new RuntimeException("testEx"));
    when(source.createIterator()).thenReturn(iterator);
    TransformationStep<Molecule, Molecule> step = TransformationStepBuilder.builder(identity).build();

    var ex = assertThrows(ExecutionException.class,
        () -> loader.loadStorageItems(source, step, consumer).toCompletableFuture().get());

    assertEquals("testEx", ex.getCause().getMessage());
  }

  @Test
  void loadMoleculesWithBadConsumer() throws Exception {
    when(source.createIterator()).thenReturn(List.of(new Molecule(), new Molecule(), new Molecule()).iterator());
    TransformationStep<Molecule, Molecule> step = TransformationStepBuilder.builder(identity).build();
    Mockito.doThrow(new RuntimeException()).when(consumer).accept(Mockito.any(Molecule.class));

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(0, stat.getCountOfSuccessfullyProcessed());
    assertEquals(3, stat.getCountOfErrors());
  }
}
