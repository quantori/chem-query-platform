package com.quantori.qdp.core.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import akka.actor.typed.ActorSystem;
import com.quantori.qdp.api.model.core.DataSource;
import com.quantori.qdp.api.model.core.DataStorage;
import com.quantori.qdp.api.model.core.MultiStorageSearchRequest;
import com.quantori.qdp.api.model.core.ProcessingSettings;
import com.quantori.qdp.api.model.core.RequestStructure;
import com.quantori.qdp.api.model.core.SearchResult;
import com.quantori.qdp.api.model.core.StorageRequest;
import com.quantori.qdp.api.model.core.TransformationStep;
import com.quantori.qdp.api.model.core.TransformationStepBuilder;
import com.quantori.qdp.api.service.ItemWriter;
import com.quantori.qdp.api.service.SearchIterator;
import com.quantori.qdp.core.configuration.ClusterConfigurationProperties;
import com.quantori.qdp.core.configuration.ClusterProvider;
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

class QdpServiceTest {
  private static final String TEST_STORAGE = "test_storage";
  private static final String TEST_STORAGE_2 = "test_storage-2";
  private static final String LIBRARY_NAME = "qdp_mol_service_name";
  private static final int MAX_UPLOADS = 3;
  public static final StorageRequest BLANK_STORAGE_REQUEST = new StorageRequest() {
  };
  public static final Function<TestStorageItem, TestSearchItem>
      RESULT_ITEM_NUMBER_FUNCTION = item -> new TestSearchItem(item.getId());
  public static final String TEST_INDEX = "testIndex";

  @SuppressWarnings("unchecked")
  @Test
  void registerMoleculeStorage() throws ExecutionException, InterruptedException {
    QdpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service = new QdpService<>();

    DataStorage<TestStorageUploadItem, TestSearchItem, TestStorageItem> storage = Mockito.mock(DataStorage.class);
    service.registerUploadStorage(storage, TEST_STORAGE, MAX_UPLOADS);
    DataStorage<TestStorageUploadItem, TestSearchItem, TestStorageItem> storage2 = Mockito.mock(DataStorage.class);
    service.registerUploadStorage(storage2, TEST_STORAGE_2, MAX_UPLOADS);

    var listOfSources = service.listSources().toCompletableFuture().get();
    assertEquals(2, listOfSources.size());

    assertEquals(1, listOfSources.stream().filter(s -> s.storageName.equals(TEST_STORAGE)).count());
    assertEquals(1, listOfSources.stream().filter(s -> s.storageName.equals(TEST_STORAGE_2)).count());
  }

  @SuppressWarnings("unchecked")
  @Test
  void loadMoleculesFromDataSource() throws Exception {
    var storage = Mockito.mock(DataStorage.class);
    var loader = Mockito.mock(ItemWriter.class);
    Mockito.doNothing().when(loader).write(Mockito.any());
    Mockito.when(storage.itemWriter(Mockito.any())).thenReturn(loader);

    DataSource<TestDataUploadItem> source = (DataSource<TestDataUploadItem>) Mockito.mock(DataSource.class);
    when(source.createIterator()).thenReturn(List.of(new TestDataUploadItem()).iterator());

    Function<TestDataUploadItem, TestStorageUploadItem> func = data -> new TestStorageUploadItem("transformed");

    TransformationStep<TestDataUploadItem, TestStorageUploadItem> step =
        TransformationStepBuilder.builder(func).build();

    QdpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service = new QdpService<>();
    service.registerUploadStorage(storage, TEST_STORAGE, MAX_UPLOADS);

    var stat = service.loadStorageItemsFromDataSource(TEST_STORAGE, LIBRARY_NAME, source, step)
        .toCompletableFuture().get();
    assertFalse(stat.isFailed());
    assertEquals(1, stat.getCountOfSuccessfullyProcessed());

    ArgumentCaptor<TestStorageUploadItem> captor = ArgumentCaptor.forClass(TestStorageUploadItem.class);
    Mockito.verify(loader).write(captor.capture());
    assertEquals("transformed", captor.getValue().getId());

    Mockito.verify(source, times(1)).createIterator();
    Mockito.verify(source, times(1)).close();
  }

  @Test
  void testSearch() {
    QdpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service = new QdpService<>();

    DataStorage<?, TestSearchItem, TestStorageItem> storage = new IntRangeDataStorage(10);
    service.registerSearchStorages(Map.of(TEST_STORAGE, storage));
    var request = MultiStorageSearchRequest.<TestSearchItem, TestStorageItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE,
            RequestStructure.<TestSearchItem, TestStorageItem>builder()
                .storageName(TEST_STORAGE)
                .indexNames(List.of(TEST_INDEX))
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
      searchResult = service.nextSearchResult(searchResult.getSearchId(), 10, "user")
          .toCompletableFuture().join();
      assertEquals(10, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      resultItems.addAll(searchResult.getResults());
    }
    for (int i = 0; i < 5; i++) {
      searchResult = service.nextSearchResult(searchResult.getSearchId(), 6, "user")
          .toCompletableFuture().join();
      assertEquals(6, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      resultItems.addAll(searchResult.getResults());
    }
    for (int i = 0; i < 2; i++) {
      searchResult = service.nextSearchResult(searchResult.getSearchId(), 7, "user")
          .toCompletableFuture().join();
      assertEquals(7, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      resultItems.addAll(searchResult.getResults());
    }
    searchResult = service.nextSearchResult(searchResult.getSearchId(), 7, "user")
        .toCompletableFuture().join();
    assertEquals(6, searchResult.getResults().size());
    assertTrue(searchResult.isSearchFinished());
    resultItems.addAll(searchResult.getResults());
    String actual = resultItems.stream().map(TestSearchItem::getId).collect(Collectors.joining(""));
    String expected = IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
  }

  @Test
  void testSearchInLoop() {
    QdpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service = new QdpService<>();

    DataStorage<?, TestSearchItem, TestStorageItem> storage = new IntRangeDataStorage(10);
    service.registerSearchStorages(Map.of(TEST_STORAGE, storage));
    var request = MultiStorageSearchRequest.<TestSearchItem, TestStorageItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE,
            RequestStructure.<TestSearchItem, TestStorageItem>builder()
                .storageName(TEST_STORAGE)
                .indexNames(List.of(TEST_INDEX))
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
      searchResult = service.nextSearchResult(searchResult.getSearchId(), 8, "user")
          .toCompletableFuture().join();
      resultItems.addAll(searchResult.getResults());
    }
    String actual = resultItems.stream().map(TestSearchItem::getId).collect(Collectors.joining(""));
    String expected = IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
  }

  @Test
  void testSearchExceptionInTransformer() {
    QdpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service = new QdpService<>();
    DataStorage<?, TestSearchItem, TestStorageItem> storage = new IntRangeDataStorage(10);
    service.registerSearchStorages(Map.of(TEST_STORAGE, storage));
    var request = MultiStorageSearchRequest.<TestSearchItem, TestStorageItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE,
            RequestStructure.<TestSearchItem, TestStorageItem>builder()
                .storageName(TEST_STORAGE)
                .indexNames(List.of(TEST_INDEX))
                .storageRequest(BLANK_STORAGE_REQUEST)
                .resultFilter(i -> true)
                .resultTransformer(item -> {
                  if (item.getId().equals("5")) {
                    throw new RuntimeException("wrong number");
                  }
                  return new TestSearchItem(item.getId());
                })
                .build()))
        .processingSettings(ProcessingSettings.builder()
            .user("user")
            .bufferSize(15)
            .parallelism(1)
            .build())
        .build();

    SearchResult<TestSearchItem> searchResult = service.search(request).toCompletableFuture().join();
    searchResult = service.nextSearchResult(searchResult.getSearchId(), 10, "user")
        .toCompletableFuture().join();
    assertEquals(10, searchResult.getResults().size());
    assertEquals(IntStream.range(0, 11).filter(i -> i != 5).mapToObj(Integer::toString).collect(Collectors.toList()),
        searchResult.getResults().stream()
            .map(TestSearchItem::getId).toList());
  }

  @Test
  void testSearchExceptionInFilter() {
    QdpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service = new QdpService<>();
    DataStorage<?, TestSearchItem, TestStorageItem> storage = new IntRangeDataStorage(10);
    service.registerSearchStorages(Map.of(TEST_STORAGE, storage));
    var request = MultiStorageSearchRequest.<TestSearchItem, TestStorageItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE,
            RequestStructure.<TestSearchItem, TestStorageItem>builder()
                .storageName(TEST_STORAGE)
                .indexNames(List.of(TEST_INDEX))
                .storageRequest(BLANK_STORAGE_REQUEST)
                .resultFilter(i -> {
                  if (Integer.parseInt(i.getId()) % 2 == 0) {
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
    searchResult = service.nextSearchResult(searchResult.getSearchId(), 10, "user")
        .toCompletableFuture().join();
    assertEquals(10, searchResult.getResults().size());
    assertEquals(
        IntStream.range(0, 20).filter(i -> (i % 2) != 0).mapToObj(Integer::toString).collect(Collectors.toList()),
        searchResult.getResults().stream()
            .map(TestSearchItem::getId).toList());
  }

  @Test
  void testSearchExceptionInDataSearcher() {
    QdpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service = new QdpService<>();
    String errorMessage = "Cannot load data";
    DataStorage<?, TestSearchItem, TestStorageItem> storage = new DataStorage<>() {
      @Override
      public List<SearchIterator<TestStorageItem>> searchIterator(
          RequestStructure<TestSearchItem, TestStorageItem> storageRequest) {
        return List.of(new SearchIterator<>() {

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
          public void close() {

          }
        });
      }
    };
    service.registerSearchStorages(Map.of(TEST_STORAGE, storage));
    var request = MultiStorageSearchRequest.<TestSearchItem, TestStorageItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE,
            RequestStructure.<TestSearchItem, TestStorageItem>builder()
                .storageName(TEST_STORAGE)
                .indexNames(List.of(TEST_INDEX))
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
    searchResult = service.nextSearchResult(searchResult.getSearchId(), 10, "user")
        .toCompletableFuture().join();
    assertFalse(searchResult.getErrors().isEmpty());
    assertTrue(searchResult.getErrors().get(0).getMessage().contains(errorMessage));

    String actual = searchResult.getResults().stream().map(TestSearchItem::getId).collect(Collectors.joining(""));
    String expected = IntStream.range(0, 5).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
  }


  @Test
  void testSearchExceptionInDataStorage() {
    QdpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service = new QdpService<>();
    String errorMessage = "Implementation error";
    DataStorage<?, TestSearchItem, TestStorageItem> storage = new DataStorage<>() {
      @Override
      public List<SearchIterator<TestStorageItem>> searchIterator(
          RequestStructure<TestSearchItem, TestStorageItem> storageRequest) {
        throw new RuntimeException(errorMessage);
      }
    };
    service.registerSearchStorages(Map.of(TEST_STORAGE, storage));
    var request = MultiStorageSearchRequest.<TestSearchItem, TestStorageItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE,
            RequestStructure.<TestSearchItem, TestStorageItem>builder()
                .storageName(TEST_STORAGE)
                .indexNames(List.of(TEST_INDEX))
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
        .seedNodes(Arrays.asList("localhost:10100", "localhost:10101"))
        .build();
    ClusterConfigurationProperties prop2 = ClusterConfigurationProperties
        .builder()
        .maxSearchActors(100)
        .clusterHostName("localhost")
        .clusterPort(10101)
        .seedNodes(Arrays.asList("localhost:10100", "localhost:10101")).build();
    ActorSystem<SourceRootActor.Command> system1 = clusterProvider.actorTypedSystem(prop1);
    ActorSystem<SourceRootActor.Command> system2 = clusterProvider.actorTypedSystem(prop2);
    try {
      List<QdpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem>> services =
          List.of(new QdpService<>(system1), new QdpService<>(system2));
      Thread.sleep(8000);
      DataStorage<?, TestSearchItem, TestStorageItem> testStorage = new IntRangeDataStorage(10);
      services.forEach(service -> service.registerSearchStorages(Map.of(TEST_STORAGE, testStorage)));
      var request = MultiStorageSearchRequest.<TestSearchItem, TestStorageItem>builder()
          .requestStorageMap(Map.of(TEST_STORAGE,
              RequestStructure.<TestSearchItem, TestStorageItem>builder()
                  .storageName(TEST_STORAGE)
                  .indexNames(List.of(TEST_INDEX))
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
      int requestCount = 0;
      SearchResult<TestSearchItem> searchResult = services.get(0).search(request)
          .thenCompose(sr -> services.get(0).nextSearchResult(sr.getSearchId(), 10,
              request.getProcessingSettings().getUser()))
          .toCompletableFuture().join();
      requestCount++;
      assertEquals(10, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      List<TestSearchItem> resultItems = new ArrayList<>(searchResult.getResults());
      for (int i = 0; i < 9; i++) {
        var storageRequest = services.get(requestCount % 2)
            .getSearchRequestDescription(searchResult.getSearchId(), TEST_STORAGE, "user")
            .toCompletableFuture().join();
        searchResult = services.get(requestCount++ % 2)
            .nextSearchResult(searchResult.getSearchId(), 10, "user")
            .toCompletableFuture().join();
        assertEquals(10, searchResult.getResults().size());
        if (i == 8) {
          assertTrue(searchResult.isSearchFinished());
        } else {
          assertFalse(searchResult.isSearchFinished());
        }

        resultItems.addAll(searchResult.getResults());
      }
      String actual = resultItems.stream().map(TestSearchItem::getId).collect(Collectors.joining(""));
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

  public static class IntRangeDataStorage
      implements DataStorage<TestStorageUploadItem, TestSearchItem, TestStorageItem> {

    private final int chunks;

    public IntRangeDataStorage(int chunks) {
      this.chunks = chunks;
    }

    @Override
    public List<SearchIterator<TestStorageItem>> searchIterator(
        RequestStructure<TestSearchItem, TestStorageItem> storageRequest) {
      return List.of(new SearchIterator<>() {
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
        public void close() {

        }
      });
    }
  }

}