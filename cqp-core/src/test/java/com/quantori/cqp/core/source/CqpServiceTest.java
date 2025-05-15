package com.quantori.cqp.core.source;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import akka.actor.typed.ActorSystem;
import com.quantori.cqp.api.DataStorage;
import com.quantori.cqp.api.ItemWriter;
import com.quantori.cqp.api.SearchIterator;
import com.quantori.cqp.api.model.StorageRequest;
import com.quantori.cqp.core.configuration.ClusterConfigurationProperties;
import com.quantori.cqp.core.configuration.ClusterProvider;
import com.quantori.cqp.core.model.DataSource;
import com.quantori.cqp.core.model.SearchRequest;
import com.quantori.cqp.core.model.SearchResult;
import com.quantori.cqp.core.model.TransformationStep;
import com.quantori.cqp.core.model.TransformationStepBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

class CqpServiceTest {
  private static final String TEST_STORAGE = "test_storage";
  private static final String TEST_STORAGE_2 = "test_storage-2";
  private static final String LIBRARY_NAME = "cqp_mol_service_name";
  private static final int MAX_UPLOADS = 3;
  public static final Function<TestStorageItem, TestSearchItem> RESULT_ITEM_NUMBER_FUNCTION =
      item -> new TestSearchItem(item.getId());
  public static final String TEST_INDEX = "testIndex";

  @SuppressWarnings("unchecked")
  @Test
  void registerMoleculeStorage() throws ExecutionException, InterruptedException {
    DataStorage<TestStorageUploadItem, TestStorageItem> storage = Mockito.mock(DataStorage.class);
    DataStorage<TestStorageUploadItem, TestStorageItem> storage2 = Mockito.mock(DataStorage.class);
    CqpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new CqpService<>(
            Map.ofEntries(Map.entry(TEST_STORAGE, storage), Map.entry(TEST_STORAGE_2, storage2)));
    var listOfSources = service.listSources().toCompletableFuture().get();
    assertEquals(2, listOfSources.size());

    assertEquals(1, listOfSources.stream().filter(s -> s.storageName.equals(TEST_STORAGE)).count());
    assertEquals(
        1, listOfSources.stream().filter(s -> s.storageName.equals(TEST_STORAGE_2)).count());
  }

  @SuppressWarnings("unchecked")
  @Test
  void loadMoleculesFromDataSource() throws Exception {
    DataStorage<TestStorageUploadItem, TestStorageItem> storage = Mockito.mock(DataStorage.class);
    var loader = Mockito.mock(ItemWriter.class);
    Mockito.doNothing().when(loader).write(Mockito.any());
    Mockito.when(storage.itemWriter(Mockito.any())).thenReturn(loader);

    DataSource<TestDataUploadItem> source =
        (DataSource<TestDataUploadItem>) Mockito.mock(DataSource.class);
    when(source.createIterator()).thenReturn(List.of(new TestDataUploadItem()).iterator());

    Function<TestDataUploadItem, TestStorageUploadItem> func =
        data -> new TestStorageUploadItem("transformed");

    TransformationStep<TestDataUploadItem, TestStorageUploadItem> step =
        TransformationStepBuilder.builder(func).build();

    CqpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new CqpService<>(Map.ofEntries(Map.entry(TEST_STORAGE, storage)));

    var stat =
        service
            .loadStorageItemsFromDataSource(TEST_STORAGE, LIBRARY_NAME, source, step)
            .toCompletableFuture()
            .get();
    assertFalse(stat.isFailed());
    assertEquals(1, stat.getCountOfSuccessfullyProcessed());

    ArgumentCaptor<TestStorageUploadItem> captor =
        ArgumentCaptor.forClass(TestStorageUploadItem.class);
    Mockito.verify(loader).write(captor.capture());
    assertEquals("transformed", captor.getValue().getId());

    Mockito.verify(source, times(1)).createIterator();
    Mockito.verify(source, times(1)).close();
  }

  @Test
  void testSearch() {
    DataStorage<TestStorageUploadItem, TestStorageItem> storage = new IntRangeDataStorage(10);
    CqpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new CqpService<>(Map.ofEntries(Map.entry(TEST_STORAGE, storage)));
    var request =
        SearchRequest.<TestSearchItem, TestStorageItem>builder()
            .requestStorageMap(
                Map.of(
                    TEST_STORAGE,
                    StorageRequest.builder()
                        .storageName(TEST_STORAGE)
                        .indexIds(List.of(TEST_INDEX))
                        .build()))
            .user("user")
            .bufferSize(100)
            .fetchLimit(100)
            .parallelism(1)
            .resultFilter(i -> true)
            .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
            .build();
    SearchResult<TestSearchItem> searchResult =
        service.search(request).toCompletableFuture().join();

    assertEquals(0, searchResult.getResults().size());
    assertFalse(searchResult.isSearchFinished());

    List<TestSearchItem> resultItems = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      searchResult =
          service
              .getNextSearchResult(searchResult.getSearchId(), 10, "user")
              .toCompletableFuture()
              .join();
      assertEquals(10, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      resultItems.addAll(searchResult.getResults());
    }
    for (int i = 0; i < 5; i++) {
      searchResult =
          service
              .getNextSearchResult(searchResult.getSearchId(), 6, "user")
              .toCompletableFuture()
              .join();
      assertEquals(6, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      resultItems.addAll(searchResult.getResults());
    }
    for (int i = 0; i < 2; i++) {
      searchResult =
          service
              .getNextSearchResult(searchResult.getSearchId(), 7, "user")
              .toCompletableFuture()
              .join();
      assertEquals(7, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      resultItems.addAll(searchResult.getResults());
    }
    searchResult =
        service
            .getNextSearchResult(searchResult.getSearchId(), 7, "user")
            .toCompletableFuture()
            .join();
    assertEquals(6, searchResult.getResults().size());
    assertTrue(searchResult.isSearchFinished());
    resultItems.addAll(searchResult.getResults());
    String actual = resultItems.stream().map(TestSearchItem::getId).collect(Collectors.joining(""));
    String expected =
        IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
  }

  @Test
  void testCountSearch() {
    DataStorage<TestStorageUploadItem, TestStorageItem> storage = new IntRangeDataStorage(10);
    CqpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new CqpService<>(Map.ofEntries(Map.entry(TEST_STORAGE, storage)));
    var request =
        SearchRequest.<TestSearchItem, TestStorageItem>builder()
            .requestStorageMap(
                Map.of(
                    TEST_STORAGE,
                    StorageRequest.builder()
                        .storageName(TEST_STORAGE)
                        .indexIds(List.of(TEST_INDEX))
                        .build()))
            .user("user")
            .bufferSize(100)
            .fetchLimit(100)
            .parallelism(1)
            .isCountTask(true)
            .resultFilter(i -> true)
            .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
            .build();
    SearchResult<TestSearchItem> searchResult =
        service.search(request).toCompletableFuture().join();

    assertEquals(0, searchResult.getResultCount());
    assertFalse(searchResult.isSearchFinished());

    String searchId = searchResult.getSearchId();
    await()
        .atMost(java.time.Duration.ofSeconds(25))
        .until(
            () ->
                service
                    .getNextSearchResult(searchId, 10, "user")
                    .toCompletableFuture()
                    .get()
                    .isSearchFinished());

    try {
      searchResult = service.getNextSearchResult(searchId, 10, "user").toCompletableFuture().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    assertEquals(100, searchResult.getFoundCount());
  }

  @Test
  void testSearchWhenFetchLimitLessThanLimitAndBufferSize() {
    DataStorage<TestStorageUploadItem, TestStorageItem> storage = new IntRangeDataStorage(10);
    CqpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new CqpService<>(Map.ofEntries(Map.entry(TEST_STORAGE, storage)));
    var request =
        SearchRequest.<TestSearchItem, TestStorageItem>builder()
            .requestStorageMap(
                Map.of(
                    TEST_STORAGE,
                    StorageRequest.builder()
                        .storageName(TEST_STORAGE)
                        .indexIds(List.of(TEST_INDEX))
                        .build()))
            .user("user")
            .bufferSize(20)
            .fetchLimit(10)
            .parallelism(1)
            .resultFilter(i -> true)
            .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
            .build();
    SearchResult<TestSearchItem> searchResult =
        service.search(request).toCompletableFuture().join();

    assertEquals(0, searchResult.getResults().size());
    assertFalse(searchResult.isSearchFinished());

    var results =
        service
            .getNextSearchResult(searchResult.getSearchId(), 20, "user")
            .toCompletableFuture()
            .join();
    assertEquals(10, results.getResults().size());
  }

  @Test
  void testSearchWhenSearchWasAborteed() {
    DataStorage<TestStorageUploadItem, TestStorageItem> storage = new IntRangeDataStorage(10);
    CqpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new CqpService<>(Map.ofEntries(Map.entry(TEST_STORAGE, storage)));
    var request =
        SearchRequest.<TestSearchItem, TestStorageItem>builder()
            .requestStorageMap(
                Map.of(
                    TEST_STORAGE,
                    StorageRequest.builder()
                        .storageName(TEST_STORAGE)
                        .indexIds(List.of(TEST_INDEX))
                        .build()))
            .user("user")
            .bufferSize(20)
            .fetchLimit(10)
            .parallelism(1)
            .resultFilter(i -> true)
            .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
            .build();
    SearchResult<TestSearchItem> searchResult =
        service.search(request).toCompletableFuture().join();

    assertEquals(0, searchResult.getResults().size());
    assertFalse(searchResult.isSearchFinished());

    service.abortSearch(searchResult.getSearchId(), "user");

    assertThrows(
        CompletionException.class,
        () ->
            service
                .getNextSearchResult(searchResult.getSearchId(), 20, "user")
                .toCompletableFuture()
                .join());
  }

  @Test
  void testSearchInLoop() {
    DataStorage<TestStorageUploadItem, TestStorageItem> storage = new IntRangeDataStorage(10);
    CqpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new CqpService<>(Map.ofEntries(Map.entry(TEST_STORAGE, storage)));
    var request =
        SearchRequest.<TestSearchItem, TestStorageItem>builder()
            .requestStorageMap(
                Map.of(
                    TEST_STORAGE,
                    StorageRequest.builder()
                        .storageName(TEST_STORAGE)
                        .indexIds(List.of(TEST_INDEX))
                        .build()))
            .user("user")
            .bufferSize(100)
            .fetchLimit(100)
            .parallelism(1)
            .resultFilter(i -> true)
            .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
            .build();
    SearchResult<TestSearchItem> searchResult =
        service.search(request).toCompletableFuture().join();
    List<TestSearchItem> resultItems = new ArrayList<>(searchResult.getResults());
    while (!searchResult.isSearchFinished()) {
      searchResult =
          service
              .getNextSearchResult(searchResult.getSearchId(), 8, "user")
              .toCompletableFuture()
              .join();
      resultItems.addAll(searchResult.getResults());
    }
    String actual = resultItems.stream().map(TestSearchItem::getId).collect(Collectors.joining(""));
    String expected =
        IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
  }

  @Test
  void testSearchExceptionInTransformer() {
    DataStorage<TestStorageUploadItem, TestStorageItem> storage = new IntRangeDataStorage(10);
    CqpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new CqpService<>(Map.ofEntries(Map.entry(TEST_STORAGE, storage)));
    var request =
        SearchRequest.<TestSearchItem, TestStorageItem>builder()
            .requestStorageMap(
                Map.of(
                    TEST_STORAGE,
                    StorageRequest.builder()
                        .storageName(TEST_STORAGE)
                        .indexIds(List.of(TEST_INDEX))
                        .build()))
            .user("user")
            .bufferSize(100)
            .fetchLimit(100)
            .parallelism(1)
            .resultFilter(i -> true)
            .resultTransformer(
                item -> {
                  if (item.getId().equals("5")) {
                    throw new RuntimeException("wrong number");
                  }
                  return new TestSearchItem(item.getId());
                })
            .build();

    SearchResult<TestSearchItem> searchResult =
        service.search(request).toCompletableFuture().join();
    searchResult =
        service
            .getNextSearchResult(searchResult.getSearchId(), 10, "user")
            .toCompletableFuture()
            .join();
    assertEquals(10, searchResult.getResults().size());
    assertEquals(
        IntStream.range(0, 11)
            .filter(i -> i != 5)
            .mapToObj(Integer::toString)
            .collect(Collectors.toList()),
        searchResult.getResults().stream().map(TestSearchItem::getId).toList());
  }

  @Test
  void testSearchExceptionInFilter() {
    DataStorage<TestStorageUploadItem, TestStorageItem> storage = new IntRangeDataStorage(10);
    CqpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new CqpService<>(Map.ofEntries(Map.entry(TEST_STORAGE, storage)));
    var request =
        SearchRequest.<TestSearchItem, TestStorageItem>builder()
            .requestStorageMap(
                Map.of(
                    TEST_STORAGE,
                    StorageRequest.builder()
                        .storageName(TEST_STORAGE)
                        .indexIds(List.of(TEST_INDEX))
                        .build()))
            .user("user")
            .bufferSize(100)
            .fetchLimit(100)
            .parallelism(1)
            .resultFilter(
                i -> {
                  if (Integer.parseInt(i.getId()) % 2 == 0) {
                    throw new RuntimeException("wrong number");
                  }
                  return true;
                })
            .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
            .build();

    SearchResult<TestSearchItem> searchResult =
        service.search(request).toCompletableFuture().join();
    searchResult =
        service
            .getNextSearchResult(searchResult.getSearchId(), 10, "user")
            .toCompletableFuture()
            .join();
    assertEquals(10, searchResult.getResults().size());
    assertEquals(
        IntStream.range(0, 20)
            .filter(i -> (i % 2) != 0)
            .mapToObj(Integer::toString)
            .collect(Collectors.toList()),
        searchResult.getResults().stream().map(TestSearchItem::getId).toList());
  }

  @Test
  void testSearchExceptionInDataSearcher() {
    String errorMessage = "Cannot load data";
    DataStorage<TestStorageUploadItem, TestStorageItem> storage =
        new DataStorage<>() {
          @Override
          public ItemWriter<TestStorageUploadItem> itemWriter(String libraryId) {
            return null;
          }

          @Override
          public List<SearchIterator<TestStorageItem>> searchIterator(
              StorageRequest storageRequest) {
            return List.of(
                new SearchIterator<>() {

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
                  public String getStorageName() {
                    return TEST_STORAGE;
                  }

                  @Override
                  public List<String> getLibraryIds() {
                    return List.of(TEST_INDEX);
                  }

                  @Override
                  public void close() {}
                });
          }
        };
    CqpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new CqpService<>(Map.ofEntries(Map.entry(TEST_STORAGE, storage)));
    var request =
        SearchRequest.<TestSearchItem, TestStorageItem>builder()
            .requestStorageMap(
                Map.of(
                    TEST_STORAGE,
                    StorageRequest.builder()
                        .storageName(TEST_STORAGE)
                        .indexIds(List.of(TEST_INDEX))
                        .build()))
            .user("user")
            .bufferSize(100)
            .fetchLimit(100)
            .parallelism(1)
            .resultFilter(i -> true)
            .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
            .build();
    SearchResult<TestSearchItem> searchResult =
        service.search(request).toCompletableFuture().join();
    searchResult =
        service
            .getNextSearchResult(searchResult.getSearchId(), 10, "user")
            .toCompletableFuture()
            .join();
    assertFalse(searchResult.getErrors().isEmpty());
    assertTrue(searchResult.getErrors().get(0).getMessage().contains(errorMessage));

    String actual =
        searchResult.getResults().stream()
            .map(TestSearchItem::getId)
            .collect(Collectors.joining(""));
    String expected =
        IntStream.range(0, 5).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
  }

  @Test
  void testSearchExceptionInDataStorage() {
    String errorMessage = "Implementation error";
    DataStorage<TestStorageUploadItem, TestStorageItem> storage =
        new DataStorage<>() {
          @Override
          public ItemWriter<TestStorageUploadItem> itemWriter(String libraryId) {
            return null;
          }

          @Override
          public List<SearchIterator<TestStorageItem>> searchIterator(
              StorageRequest storageRequest) {
            throw new RuntimeException(errorMessage);
          }
        };
    CqpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new CqpService<>(Map.ofEntries(Map.entry(TEST_STORAGE, storage)));
    var request =
        SearchRequest.<TestSearchItem, TestStorageItem>builder()
            .requestStorageMap(
                Map.of(
                    TEST_STORAGE,
                    StorageRequest.builder()
                        .storageName(TEST_STORAGE)
                        .indexIds(List.of(TEST_INDEX))
                        .build()))
            .bufferSize(100)
            .fetchLimit(100)
            .parallelism(1)
            .resultFilter(i -> true)
            .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
            .build();
    CompletionException completionException =
        assertThrows(
            CompletionException.class, () -> service.search(request).toCompletableFuture().join());
    assertTrue(completionException.getMessage().contains(errorMessage));
  }

  @Test
  void testClusterSearch() throws InterruptedException, TimeoutException {
    ClusterProvider clusterProvider = new ClusterProvider();
    ClusterConfigurationProperties prop1 =
        ClusterConfigurationProperties.builder()
            .maxSearchActors(100)
            .clusterHostName("localhost")
            .clusterPort(10100)
            .seedNodes(Arrays.asList("localhost:10100", "localhost:10101"))
            .build();
    ClusterConfigurationProperties prop2 =
        ClusterConfigurationProperties.builder()
            .maxSearchActors(100)
            .clusterHostName("localhost")
            .clusterPort(10101)
            .seedNodes(Arrays.asList("localhost:10100", "localhost:10101"))
            .build();
    ActorSystem<SourceRootActor.Command> system1 = clusterProvider.actorTypedSystem(prop1);
    ActorSystem<SourceRootActor.Command> system2 = clusterProvider.actorTypedSystem(prop2);
    try {
      DataStorage<TestStorageUploadItem, TestStorageItem> testStorage = new IntRangeDataStorage(10);
      List<CqpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem>>
          services =
              List.of(
                  new CqpService<>(
                      Map.ofEntries(Map.entry(TEST_STORAGE, testStorage)), 10, system1),
                  new CqpService<>(
                      Map.ofEntries(Map.entry(TEST_STORAGE, testStorage)), 10, system2));
      Thread.sleep(8000);
      var request =
          SearchRequest.<TestSearchItem, TestStorageItem>builder()
              .requestStorageMap(
                  Map.of(
                      TEST_STORAGE,
                      StorageRequest.builder()
                          .storageName(TEST_STORAGE)
                          .indexIds(List.of(TEST_INDEX))
                          .build()))
              .user("user")
              .bufferSize(100)
              .fetchLimit(100)
              .parallelism(1)
              .resultFilter(i -> true)
              .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
              .build();
      int requestCount = 0;
      SearchResult<TestSearchItem> searchResult =
          services
              .get(0)
              .search(request)
              .thenCompose(
                  sr ->
                      services.get(0).getNextSearchResult(sr.getSearchId(), 10, request.getUser()))
              .toCompletableFuture()
              .join();
      requestCount++;
      assertEquals(10, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      List<TestSearchItem> resultItems = new ArrayList<>(searchResult.getResults());
      for (int i = 0; i < 9; i++) {
        var storageRequest =
            services
                .get(requestCount % 2)
                .getSearchRequestDescription(searchResult.getSearchId(), TEST_STORAGE, "user")
                .toCompletableFuture()
                .join();
        searchResult =
            services
                .get(requestCount++ % 2)
                .getNextSearchResult(searchResult.getSearchId(), 10, "user")
                .toCompletableFuture()
                .join();
        assertEquals(10, searchResult.getResults().size());
        if (i == 8) {
          assertTrue(searchResult.isSearchFinished());
        } else {
          assertFalse(searchResult.isSearchFinished());
        }

        resultItems.addAll(searchResult.getResults());
      }
      String actual =
          resultItems.stream().map(TestSearchItem::getId).collect(Collectors.joining(""));
      String expected =
          IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
      assertEquals(expected, actual);
    } finally {
      system1.terminate();
      system2.terminate();
      Await.result(system1.whenTerminated(), Duration.apply(5, TimeUnit.SECONDS));
      Await.result(system2.whenTerminated(), Duration.apply(5, TimeUnit.SECONDS));
    }
  }

  public static class IntRangeDataStorage
      implements DataStorage<TestStorageUploadItem, TestStorageItem> {

    private final int chunks;

    public IntRangeDataStorage(int chunks) {
      this.chunks = chunks;
    }

    @Override
    public ItemWriter<TestStorageUploadItem> itemWriter(String libraryId) {
      return null;
    }

    @Override
    public List<SearchIterator<TestStorageItem>> searchIterator(StorageRequest storageRequest) {
      return List.of(
          new SearchIterator<>() {
            int counter;

            @Override
            public List<TestStorageItem> next() {
              if (counter >= chunks) {
                return List.of();
              } else {
                List<TestStorageItem> result =
                    IntStream.range(counter * 10, (counter + 1) * 10)
                        .mapToObj(TestStorageItem::new)
                        .collect(Collectors.toList());
                counter++;
                return result;
              }
            }

            @Override
            public String getStorageName() {
              return TEST_STORAGE;
            }

            @Override
            public List<String> getLibraryIds() {
              return List.of(TEST_INDEX);
            }

            @Override
            public void close() {}
          });
    }
  }
}
