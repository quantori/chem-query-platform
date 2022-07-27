package com.quantori.qdp.core.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import akka.actor.typed.ActorSystem;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.quantori.qdp.core.configuration.ClusterConfigurationProperties;
import com.quantori.qdp.core.configuration.ClusterProvider;
import com.quantori.qdp.core.source.model.*;

import java.util.*;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

class QdpServiceTest {
  private static final String TEST_STORAGE = "test_storage";
  private static final String TEST_STORAGE_2 = "test_storage-2";
  private static final String LIBRARY_NAME = "qdp_mol_service_name";
  private static final int MAX_UPLOADS = 3;
  public static final StorageRequest BLANK_STORAGE_REQUEST = new StorageRequest() {
  };
  public static final Function<StorageItem, TestSearchItem>
      RESULT_ITEM_NUMBER_FUNCTION = i -> new TestSearchItem(((TestStorageItem) i).getNumber());

  static Stream<FetchWaitMode> waitModes() {
    return Arrays.stream(
        FetchWaitMode.values());
  }

  @SuppressWarnings("unchecked")
  @Test
  void registerMoleculeStorage() throws ExecutionException, InterruptedException {
    QdpService service = new QdpService();

    DataStorage<Molecule> storage = Mockito.mock(DataStorage.class);
    service.registerStorage(storage, TEST_STORAGE, MAX_UPLOADS);
    DataStorage<Molecule> storage2 = Mockito.mock(DataStorage.class);
    service.registerStorage(storage2, TEST_STORAGE_2, MAX_UPLOADS);

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
    when(source.createIterator()).thenReturn(List.of(new Molecule()).iterator());

    Function<Molecule, Molecule> func = (qdpMolecule) -> {
      qdpMolecule.setId("transformed");
      return qdpMolecule;
    };

    TransformationStep<Molecule, Molecule> step = TransformationStepBuilder.builder(func).build();

    QdpService service = new QdpService();
    service.registerStorage(storage, TEST_STORAGE, MAX_UPLOADS);

    var stat = service.loadStorageItemsFromDataSource(TEST_STORAGE, LIBRARY_NAME, source, step)
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
    QdpService service = new QdpService();
    DataLibrary index = new DataLibrary(LIBRARY_NAME, DataLibraryType.MOLECULE, Map.of());
    DataStorage<Molecule> storage = Mockito.mock(DataStorage.class);
    Mockito.when(storage.getLibraries()).thenReturn(List.of(index));

    service.registerStorage(storage, TEST_STORAGE, MAX_UPLOADS);
    service.createDataStorageIndex(TEST_STORAGE, index);
    var result = service.getDataStorageIndexes(TEST_STORAGE).toCompletableFuture().get();

    ArgumentCaptor<DataLibrary> valueCapture = ArgumentCaptor.forClass(DataLibrary.class);
    Mockito.verify(storage).createLibrary(valueCapture.capture());
    assertEquals(LIBRARY_NAME, valueCapture.getValue().getName());
    assertEquals(1, result.size());
    assertEquals(LIBRARY_NAME, result.get(0).getName());
  }

  @Test
  void testSearch() {
    QdpService service = new QdpService();

    DataStorage<TestStorageItem> storage = new IntRangeDataStorage(10);
    service.registerStorage(Map.of(TEST_STORAGE, storage));
    var request = MultiStorageSearchRequest.<TestSearchItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE)
                .indexNames(List.of("testIndex"))
                .storageRequest(BLANK_STORAGE_REQUEST)
                .resultFilter(i -> true)
                .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
                .build()))
        .processingSettings(ProcessingSettings.builder()
            .user("user")
            .bufferSize(15)
            .parallelism(1)
            .build())
        .build();
    SearchResult<TestSearchItem> searchResult = service.search(request).toCompletableFuture().join();

    assertEquals(0, searchResult.getResults().size());
    assertFalse(searchResult.isSearchFinished());

    List<TestSearchItem> resultItems = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      searchResult = service.<TestSearchItem>nextSearchResult(searchResult.getSearchId(), 10, "user")
          .toCompletableFuture().join();
      assertEquals(10, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      resultItems.addAll(searchResult.getResults());
    }
    for (int i = 0; i < 5; i++) {
      searchResult = service.<TestSearchItem>nextSearchResult(searchResult.getSearchId(), 6, "user")
          .toCompletableFuture().join();
      assertEquals(6, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      resultItems.addAll(searchResult.getResults());
    }
    for (int i = 0; i < 2; i++) {
      searchResult = service.<TestSearchItem>nextSearchResult(searchResult.getSearchId(), 7, "user")
          .toCompletableFuture().join();
      assertEquals(7, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      resultItems.addAll(searchResult.getResults());
    }
    searchResult = service.<TestSearchItem>nextSearchResult(searchResult.getSearchId(), 7, "user")
        .toCompletableFuture().join();
    assertEquals(6, searchResult.getResults().size());
    assertTrue(searchResult.isSearchFinished());
    resultItems.addAll(searchResult.getResults());
    String actual = resultItems.stream().map(TestSearchItem::getNumber).collect(Collectors.joining(""));
    String expected = IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
  }


  @ParameterizedTest(name = "testSearchInLoop ({arguments})")
  @MethodSource("waitModes")
  void testSearchInLoop(FetchWaitMode fetchWaitMode) {
    QdpService service = new QdpService();

    DataStorage<TestStorageItem> storage = new IntRangeDataStorage(10);
    service.registerStorage(Map.of(TEST_STORAGE, storage));
    var request = MultiStorageSearchRequest.<TestSearchItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE)
                .indexNames(List.of("testIndex"))
                .storageRequest(BLANK_STORAGE_REQUEST)
                .resultFilter(i -> true)
                .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
                .build()))
        .processingSettings(ProcessingSettings.builder()
            .user("user")
            .bufferSize(15)
            .parallelism(1)
            .build())
        .build();
    SearchResult<TestSearchItem> searchResult = service.search(request).toCompletableFuture().join();
    List<TestSearchItem> resultItems = new ArrayList<>(searchResult.getResults());
    while (!searchResult.isSearchFinished()) {
      searchResult = service.<TestSearchItem>nextSearchResult(searchResult.getSearchId(), 8, "user")
          .toCompletableFuture().join();
      resultItems.addAll(searchResult.getResults());
    }
    String actual = resultItems.stream().map(TestSearchItem::getNumber).collect(Collectors.joining(""));
    String expected = IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
  }

  @Test
  void testSearchExceptionInTransformer() {
    QdpService service = new QdpService();
    DataStorage<TestStorageItem> storage = new IntRangeDataStorage(10);
    service.registerStorage(Map.of(TEST_STORAGE, storage));
    var request = MultiStorageSearchRequest.<TestSearchItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE)
                .indexNames(List.of("testIndex"))
                .storageRequest(BLANK_STORAGE_REQUEST)
                .resultFilter(i -> true)
                .resultTransformer(i -> {
                  TestSearchItem result = new TestSearchItem(((TestStorageItem) i).getNumber());
                  if (result.getNumber().equals("5")) {
                    throw new RuntimeException("wrong number");
                  }
                  return result;
                })
                .build()))
        .processingSettings(ProcessingSettings.builder()
            .user("user")
            .bufferSize(15)
            .parallelism(1)
            .build())
        .build();

    SearchResult<TestSearchItem> searchResult = service.search(request).toCompletableFuture().join();
    searchResult = service.<TestSearchItem>nextSearchResult(searchResult.getSearchId(), 10, "user")
        .toCompletableFuture().join();
    assertEquals(10, searchResult.getResults().size());
    assertEquals(IntStream.range(0, 11).filter(i -> i != 5).mapToObj(Integer::toString).collect(Collectors.toList()),
        searchResult.getResults().stream()
            .map(TestSearchItem::getNumber).toList());
  }

  @Test
  void testSearchExceptionInFilter() {
    QdpService service = new QdpService();
    DataStorage<TestStorageItem> storage = new IntRangeDataStorage(10);
    service.registerStorage(Map.of(TEST_STORAGE, storage));
    var request = MultiStorageSearchRequest.<TestSearchItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE)
                .indexNames(List.of("testIndex"))
                .storageRequest(BLANK_STORAGE_REQUEST)
                .resultFilter(i -> {
                  if (((TestStorageItem) i).getNumber() % 2 == 0) {
                    throw new RuntimeException("wrong number");
                  }
                  return true;
                })
                .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
                .build()))
        .processingSettings(ProcessingSettings.builder()
            .user("user")
            .bufferSize(15)
            .parallelism(1)
            .build())
        .build();

    SearchResult<TestSearchItem> searchResult = service.search(request).toCompletableFuture().join();
    searchResult = service.<TestSearchItem>nextSearchResult(searchResult.getSearchId(), 10, "user")
        .toCompletableFuture().join();
    assertEquals(10, searchResult.getResults().size());
    assertEquals(
        IntStream.range(0, 20).filter(i -> (i % 2) != 0).mapToObj(Integer::toString).collect(Collectors.toList()),
        searchResult.getResults().stream()
            .map(TestSearchItem::getNumber).toList());
  }

  @Test
  void testSearchExceptionInDataSearcher() {
    QdpService service = new QdpService();
    String errorMessage = "Cannot load data";
    DataStorage<TestStorageItem> storage = new DataStorage<>() {
      @Override
      public DataSearcher dataSearcher(RequestStructure storageRequest) {
        return new DataSearcher() {

          int count;

          @Override
          public List<TestStorageItem> next() {
            if (count == 0) {
              count++;
              return IntStream.range(0, 5)
                  .mapToObj(TestStorageItem::new)
                  .collect(Collectors.toList());

            } else {
              throw new RuntimeException(errorMessage);
            }
          }

          @Override
          public void close() {

          }
        };
      }
    };
    service.registerStorage(Map.of(TEST_STORAGE, storage));
    var request = MultiStorageSearchRequest.<TestSearchItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE)
                .indexNames(List.of("testIndex"))
                .storageRequest(BLANK_STORAGE_REQUEST)
                .resultFilter(i -> true)
                .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
                .build()))
        .processingSettings(ProcessingSettings.builder()
            .user("user")
            .bufferSize(15)
            .parallelism(1)
            .build())
        .build();
    SearchResult<TestSearchItem> searchResult = service.search(request).toCompletableFuture().join();
    CompletionException completionException = assertThrows(CompletionException.class, () ->
        service.nextSearchResult(searchResult.getSearchId(), 10, "user").toCompletableFuture().join());
    assertTrue(completionException.getMessage().contains(errorMessage));
  }


  @Test
  void testSearchExceptionInDataStorage() {
    QdpService service = new QdpService();
    String errorMessage = "Implementation error";
    DataStorage<TestStorageItem> storage = new DataStorage<>() {
      @Override
      public DataSearcher dataSearcher(RequestStructure storageRequest) {
        throw new RuntimeException(errorMessage);
      }
    };
    service.registerStorage(Map.of(TEST_STORAGE, storage));
    var request = MultiStorageSearchRequest.<TestSearchItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE)
                .indexNames(List.of("testIndex"))
                .storageRequest(BLANK_STORAGE_REQUEST)
                .resultFilter(i -> true)
                .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
                .build()))
        .processingSettings(ProcessingSettings.builder()
            .bufferSize(15)
            .parallelism(1)
            .build())
        .build();
    CompletionException completionException =
        assertThrows(CompletionException.class, () -> service.search(request).toCompletableFuture().join());
    assertTrue(completionException.getMessage().contains(errorMessage));
  }

  @Test
  void testClusterSearch() throws InterruptedException, TimeoutException {
    ClusterProvider clusterProvider = new ClusterProvider();
    ClusterConfigurationProperties prop1 = ClusterConfigurationProperties
            .builder()
            .maxSearchActors(100)
            .clusterHostName("localhost")
            .clusterPort(10100)
            .seedNodes(Arrays.asList("localhost:10100","localhost:10101"))
            .build();
    ClusterConfigurationProperties prop2 = ClusterConfigurationProperties
            .builder()
            .maxSearchActors(100)
            .clusterHostName("localhost")
            .clusterPort(10101)
            .seedNodes(Arrays.asList("localhost:10100","localhost:10101")).build();
    ActorSystem<SourceRootActor.Command> system1 = clusterProvider.actorTypedSystem(prop1);
    ActorSystem<SourceRootActor.Command> system2 = clusterProvider.actorTypedSystem(prop2);
    try {
      QdpService[] services = new QdpService[]{new QdpService(system1),
              new QdpService(system2)};
      Thread.sleep(8000);
      DataStorage<TestStorageItem> testStorage = new IntRangeDataStorage(10);
      for (int i = 0; i < 2; i++) {
        services[i].registerStorage(Map.of(TEST_STORAGE, testStorage));
      }
      var request = MultiStorageSearchRequest.<TestSearchItem>builder()
              .requestStorageMap(Map.of(TEST_STORAGE,
                      RequestStructure.<TestSearchItem>builder()
                              .storageName(TEST_STORAGE)
                              .indexNames(List.of("testIndex"))
                              .storageRequest(new FakeRequest())
                              .resultFilter(i -> true)
                              .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
                              .build()))
              .processingSettings(ProcessingSettings.builder()
                      .user("user")
                      .bufferSize(15)
                      .parallelism(1)
                      .build())
              .build();
      List<TestSearchItem> resultItems = new ArrayList<>();
      int requestCount = 0;
      SearchResult<TestSearchItem> searchResult = services[0].search(request)
              .thenCompose(sr -> services[0].<TestSearchItem>nextSearchResult(sr.getSearchId(),
                      10,
                      request.getProcessingSettings().getUser()))
              .toCompletableFuture().join();
      requestCount++;
      assertEquals(10, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      resultItems.addAll(searchResult.getResults());
      for (int i = 0; i < 9; i++) {
        System.out.println("Request " + i);
        StorageRequest storageRequest = services[requestCount % 2].getSearchRequestDescription(searchResult.getSearchId(), TEST_STORAGE, "user").toCompletableFuture().join();
        searchResult = services[requestCount++ % 2].<TestSearchItem>nextSearchResult(searchResult.getSearchId(), 10, "user").toCompletableFuture().join();
        assertEquals(10, searchResult.getResults().size());
        if (i == 8) {
          assertTrue(searchResult.isSearchFinished());
        } else {
          assertFalse(searchResult.isSearchFinished());
        }

        resultItems.addAll(searchResult.getResults());
      }
      String actual = resultItems.stream().map(item -> item.getNumber()).collect(Collectors.joining(""));
      String expected = IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
      assertEquals(expected, actual);
    } finally {
      system1.terminate();
      system2.terminate();
      Await.result(system1.whenTerminated(), Duration.apply(5, TimeUnit.SECONDS));
      Await.result(system2.whenTerminated(), Duration.apply(5, TimeUnit.SECONDS));
    }
  }

  public static class FakeRequest implements StorageRequest {

  }

  public static class TestStorageItem implements StorageItem {
    private final int number;

    public TestStorageItem(int i) {
      this.number = i;
    }

    public int getNumber() {
      return number;
    }
  }

  public static class TestSearchItem implements SearchItem {
    private final String number;

    @JsonCreator
    public TestSearchItem(int number) {
      this.number = Integer.toString(number);
    }

    public String getNumber() {
      return number;
    }

    @Override
    public String toString() {
      return "SearchItem{" +
          "number='" + number + '\'' +
          '}';
    }
  }

  public static class IntRangeDataStorage implements DataStorage<TestStorageItem> {

    private final int chunks;

    public IntRangeDataStorage(int chunks) {
      this.chunks = chunks;
    }

    @Override
    public DataSearcher dataSearcher(RequestStructure storageRequest) {
      return new DataSearcher() {
        int counter;

        @Override
        public List<? extends StorageItem> next() {
          if (counter >= chunks) {
            return List.of();
          } else {
            List<StorageItem> result =
                IntStream.range(counter * 10, (counter + 1) * 10)
                    .mapToObj(TestStorageItem::new)
                    .collect(Collectors.toList());
            counter++;
            return result;
          }
        }

        @Override
        public void close() throws Exception {

        }
      };
    }
  }

}