package com.quantori.cqp.core.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import com.quantori.cqp.core.model.DataSource;
import com.quantori.cqp.core.model.TransformationStep;
import com.quantori.cqp.core.model.TransformationStepBuilder;
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
  static final Function<TestDataUploadItem, TestStorageUploadItem> IDENTITY =
      item -> new TestStorageUploadItem(item.getId());
  static final Function<TestDataUploadItem, TestStorageUploadItem> TRANSFORM =
      item -> new TestStorageUploadItem("transformed");

  private Loader<TestDataUploadItem, TestStorageUploadItem> loader;
  private DataSource<TestDataUploadItem> source;
  private Consumer<TestStorageUploadItem> consumer;

  @AfterAll
  public static void teardown() {
    testKit.shutdownTestKit();
  }

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() {
    loader = new Loader<>(testKit.system());
    source = Mockito.mock(DataSource.class);
    consumer = Mockito.mock(Consumer.class);
  }

  @Test
  void loadOneMolecule() throws Exception {
    when(source.createIterator()).thenReturn(List.of(new TestDataUploadItem()).iterator());
    TransformationStep<TestDataUploadItem, TestStorageUploadItem> step =
        TransformationStepBuilder.builder(TRANSFORM).build();

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(1, stat.getCountOfSuccessfullyProcessed());
    assertEquals(0, stat.getCountOfErrors());

    ArgumentCaptor<TestStorageUploadItem> captor =
        ArgumentCaptor.forClass(TestStorageUploadItem.class);
    Mockito.verify(consumer).accept(captor.capture());
    assertEquals("transformed", captor.getValue().getId());
  }

  @Test
  void loadSeveralMolecules() throws Exception {
    when(source.createIterator())
        .thenReturn(
            List.of(new TestDataUploadItem(), new TestDataUploadItem(), new TestDataUploadItem())
                .iterator());
    TransformationStep<TestDataUploadItem, TestStorageUploadItem> step =
        TransformationStepBuilder.builder(TRANSFORM).build();

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(3, stat.getCountOfSuccessfullyProcessed());
    assertEquals(0, stat.getCountOfErrors());

    ArgumentCaptor<TestStorageUploadItem> captor =
        ArgumentCaptor.forClass(TestStorageUploadItem.class);
    Mockito.verify(consumer, times(3)).accept(captor.capture());
    captor.getAllValues().forEach(mol -> assertEquals("transformed", mol.getId()));
  }

  @Test
  void loadSkipTransformationErrors() throws Exception {
    when(source.createIterator())
        .thenReturn(
            List.of(
                    new TestDataUploadItem(),
                    new TestDataUploadItem("error"),
                    new TestDataUploadItem())
                .iterator());
    Function<TestDataUploadItem, TestStorageUploadItem> func =
        (item) -> {
          if ("error".equals(item.getId())) {
            throw new RuntimeException("test");
          }
          return new TestStorageUploadItem("transformed");
        };
    TransformationStep<TestDataUploadItem, TestStorageUploadItem> step =
        TransformationStepBuilder.builder(func).build();

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(2, stat.getCountOfSuccessfullyProcessed());
    assertEquals(1, stat.getCountOfErrors());

    ArgumentCaptor<TestStorageUploadItem> captor =
        ArgumentCaptor.forClass(TestStorageUploadItem.class);
    Mockito.verify(consumer, times(2)).accept(captor.capture());
    captor.getAllValues().forEach(item -> assertEquals("transformed", item.getId()));
  }

  @Test
  void loadSkipConsumerErrors() throws Exception {
    when(source.createIterator())
        .thenReturn(
            List.of(
                    new TestDataUploadItem(),
                    new TestDataUploadItem("error"),
                    new TestDataUploadItem())
                .iterator());
    Function<TestDataUploadItem, TestStorageUploadItem> func =
        (item) -> {
          if ("error".equals(item.getId())) {
            return new TestStorageUploadItem(item.getId());
          }
          return new TestStorageUploadItem("transformed");
        };
    TransformationStep<TestDataUploadItem, TestStorageUploadItem> step =
        TransformationStepBuilder.builder(func).build();
    doAnswer(
            invocation -> {
              TestStorageUploadItem testUploadItem = invocation.getArgument(0);
              if ("error".equals(testUploadItem.getId())) {
                throw new RuntimeException("test");
              }
              return null;
            })
        .when(consumer)
        .accept(any(TestStorageUploadItem.class));

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(2, stat.getCountOfSuccessfullyProcessed());
    assertEquals(1, stat.getCountOfErrors());

    Mockito.verify(consumer, times(3)).accept(any(TestStorageUploadItem.class));
  }

  @Test
  void loadZeroMolecules() throws Exception {
    when(source.createIterator()).thenReturn(Collections.emptyIterator());
    TransformationStep<TestDataUploadItem, TestStorageUploadItem> step =
        TransformationStepBuilder.builder(IDENTITY).build();

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(0, stat.getCountOfSuccessfullyProcessed());
    assertEquals(0, stat.getCountOfErrors());

    ArgumentCaptor<TestStorageUploadItem> captor =
        ArgumentCaptor.forClass(TestStorageUploadItem.class);
    Mockito.verify(consumer, times(0)).accept(captor.capture());
  }

  @Test
  void loadNulMolecules() {
    List<TestDataUploadItem> list = new ArrayList<>();
    list.add(new TestDataUploadItem());
    list.add(null);
    list.add(new TestDataUploadItem());
    when(source.createIterator()).thenReturn(list.iterator());
    TransformationStep<TestDataUploadItem, TestStorageUploadItem> step =
        TransformationStepBuilder.builder(IDENTITY).build();

    var ex =
        assertThrows(
            ExecutionException.class,
            () -> loader.loadStorageItems(source, step, consumer).toCompletableFuture().get());

    assertEquals("Element must not be null, rule 2.13", ex.getCause().getMessage());
  }

  @Test
  void loadNulInTransformationStep() throws Exception {
    when(source.createIterator())
        .thenReturn(
            List.of(
                    new TestDataUploadItem(),
                    new TestDataUploadItem("error"),
                    new TestDataUploadItem())
                .iterator());
    Function<TestDataUploadItem, TestStorageUploadItem> func =
        (item) -> {
          if ("error".equals(item.getId())) {
            return null;
          }
          return new TestStorageUploadItem("transformed");
        };
    TransformationStep<TestDataUploadItem, TestStorageUploadItem> step =
        TransformationStepBuilder.builder(func).build();

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(2, stat.getCountOfSuccessfullyProcessed());
    assertEquals(0, stat.getCountOfErrors());

    ArgumentCaptor<TestStorageUploadItem> captor =
        ArgumentCaptor.forClass(TestStorageUploadItem.class);
    Mockito.verify(consumer, times(2)).accept(captor.capture());
  }

  @Test
  void loadWithExceptionInTransformationStep() throws Exception {
    when(source.createIterator())
        .thenReturn(
            List.of(new TestDataUploadItem(), new TestDataUploadItem(), new TestDataUploadItem())
                .iterator());
    Function<TestDataUploadItem, TestStorageUploadItem> func =
        (item) -> {
          throw new RuntimeException("test");
        };
    TransformationStep<TestDataUploadItem, TestStorageUploadItem> step =
        TransformationStepBuilder.builder(func).build();

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(0, stat.getCountOfSuccessfullyProcessed());
    assertEquals(3, stat.getCountOfErrors());
  }

  @Test
  void loadMoleculesWithBadSource() {
    when(source.createIterator()).thenThrow(new RuntimeException("testEx"));
    TransformationStep<TestDataUploadItem, TestStorageUploadItem> step =
        TransformationStepBuilder.builder(IDENTITY).build();

    var ex =
        assertThrows(
            ExecutionException.class,
            () -> loader.loadStorageItems(source, step, consumer).toCompletableFuture().get());

    MatcherAssert.assertThat(ex.getCause().getMessage(), CoreMatchers.startsWith("testEx"));
  }

  @SuppressWarnings("unchecked")
  @Test
  void loadMoleculesWithBadSourceIterator() {
    Iterator<TestDataUploadItem> iterator = Mockito.mock(Iterator.class);
    when(iterator.hasNext()).thenReturn(true);
    when(iterator.next()).thenThrow(new RuntimeException("testEx"));
    when(source.createIterator()).thenReturn(iterator);
    TransformationStep<TestDataUploadItem, TestStorageUploadItem> step =
        TransformationStepBuilder.builder(IDENTITY).build();

    var ex =
        assertThrows(
            ExecutionException.class,
            () -> loader.loadStorageItems(source, step, consumer).toCompletableFuture().get());

    assertEquals("testEx", ex.getCause().getMessage());
  }

  @Test
  void loadMoleculesWithBadConsumer() throws Exception {
    when(source.createIterator())
        .thenReturn(
            List.of(new TestDataUploadItem(), new TestDataUploadItem(), new TestDataUploadItem())
                .iterator());
    TransformationStep<TestDataUploadItem, TestStorageUploadItem> step =
        TransformationStepBuilder.builder(IDENTITY).build();
    Mockito.doThrow(new RuntimeException())
        .when(consumer)
        .accept(Mockito.any(TestStorageUploadItem.class));

    var stat = loader.loadStorageItems(source, step, consumer).toCompletableFuture().get();

    assertEquals(0, stat.getCountOfSuccessfullyProcessed());
    assertEquals(3, stat.getCountOfErrors());
  }
}
