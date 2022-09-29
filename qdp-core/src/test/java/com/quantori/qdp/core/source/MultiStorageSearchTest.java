package com.quantori.qdp.core.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.actor.typed.ActorSystem;
import com.quantori.qdp.core.configuration.ClusterConfigurationProperties;
import com.quantori.qdp.core.configuration.ClusterProvider;
import com.quantori.qdp.core.source.model.DataSearcher;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.MultiStorageSearchRequest;
import com.quantori.qdp.core.source.model.ProcessingSettings;
import com.quantori.qdp.core.source.model.RequestStructure;
import com.quantori.qdp.core.source.model.SearchResult;
import com.quantori.qdp.core.source.model.StorageItem;
import com.quantori.qdp.core.source.model.StorageRequest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;


public class MultiStorageSearchTest {
  private static final String TEST_STORAGE_1 = "test_storage_1";
  private static final String TEST_STORAGE_2 = "test_storage_2";
  public static final StorageRequest BLANK_STORAGE_REQUEST = new StorageRequest() {
  };
  public static final Function<StorageItem, TestSearchItem>
      RESULT_ITEM_NUMBER_FUNCTION =
      i -> new TestSearchItem(((TestStorageItem) i).getNumber());
  public static final Comparator<TestSearchItem> comparator =
      Comparator.comparingInt(o -> Integer.parseInt(o.getNumber()));
  public static final String TEST_INDEX_1 = "testIndex1";
  public static final String TEST_INDEX_2 = "testIndex2";
  public static final String TEST_INDEX_3 = "testIndex3";
  public static final String TEST_INDEX_4 = "testIndex4";

  @Test
  void testSearch() {
    QdpService service = new QdpService();

    DataStorage<TestStorageItem> storage = new OddIntRangeDataStorage(10);
    DataStorage<TestStorageItem> secondStorage = new EvenIntRangeDataStorage(10);
    service.registerSearchStorages(
        Map.ofEntries(Map.entry(TEST_STORAGE_1, storage), Map.entry(TEST_STORAGE_2, secondStorage)));
    var request = MultiStorageSearchRequest.<TestSearchItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE_1,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE_1)
                .indexNames(List.of(TEST_INDEX_1, TEST_INDEX_2))
                .storageRequest(BLANK_STORAGE_REQUEST)
                .resultFilter(i -> true)
                .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
                .build(),
            TEST_STORAGE_2,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE_2)
                .indexNames(List.of(TEST_INDEX_3, TEST_INDEX_4))
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
    String actual =
        resultItems.stream().sorted(comparator).map(TestSearchItem::getNumber).collect(Collectors.joining(""));
    String expected = IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
  }

  @Test
  void testSearchInLoop() {
    QdpService service = new QdpService();

    DataStorage<TestStorageItem> storage = new OddIntRangeDataStorage(10);
    DataStorage<TestStorageItem> secondStorage = new EvenIntRangeDataStorage(10);
    service.registerSearchStorages(
        Map.ofEntries(Map.entry(TEST_STORAGE_1, storage), Map.entry(TEST_STORAGE_2, secondStorage)));
    var request = MultiStorageSearchRequest.<TestSearchItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE_1,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE_1)
                .indexNames(List.of(TEST_INDEX_1, TEST_INDEX_2))
                .storageRequest(BLANK_STORAGE_REQUEST)
                .resultFilter(i -> true)
                .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
                .build(),
            TEST_STORAGE_2,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE_2)
                .indexNames(List.of(TEST_INDEX_3, TEST_INDEX_4))
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

    String actual =
        resultItems.stream().sorted(comparator).map(TestSearchItem::getNumber).collect(Collectors.joining(""));
    String expected = IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
  }

  @Test
  void testSearchExceptionInTransformer() {
    QdpService service = new QdpService();
    DataStorage<TestStorageItem> storage = new OddIntRangeDataStorage(2);
    DataStorage<TestStorageItem> secondStorage = new EvenIntRangeDataStorage(2);
    service.registerSearchStorages(
        Map.ofEntries(Map.entry(TEST_STORAGE_1, storage), Map.entry(TEST_STORAGE_2, secondStorage)));
    var request = MultiStorageSearchRequest.<TestSearchItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE_1,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE_1)
                .indexNames(List.of(TEST_INDEX_1, TEST_INDEX_2))
                .storageRequest(BLANK_STORAGE_REQUEST)
                .resultFilter(i -> true)
                .resultTransformer(i -> {
                  TestSearchItem result = new TestSearchItem(((TestStorageItem) i).getNumber());
                  if (result.getNumber().equals("5")) {
                    throw new RuntimeException("wrong number");
                  }
                  return result;
                })
                .build(),
            TEST_STORAGE_2,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE_2)
                .indexNames(List.of(TEST_INDEX_3, TEST_INDEX_4))
                .storageRequest(BLANK_STORAGE_REQUEST)
                .resultFilter(i -> true)
                .resultTransformer(i -> {
                  TestSearchItem result = new TestSearchItem(((TestStorageItem) i).getNumber());
                  if (result.getNumber().equals("2")) {
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
    searchResult = service.<TestSearchItem>nextSearchResult(searchResult.getSearchId(), 20, "user")
        .toCompletableFuture().join();
    assertEquals(18, searchResult.getResults().size());
    String actual =
        searchResult.getResults().stream().sorted(comparator).map(TestSearchItem::getNumber)
            .collect(Collectors.joining(""));
    String expected = IntStream.range(0, 20).filter(i -> ((i != 5) && (i != 2))).mapToObj(Integer::toString)
        .collect(Collectors.joining(""));
    assertEquals(actual, expected);
  }

  @Test
  void testSearchExceptionInFilter() {
    QdpService service = new QdpService();
    DataStorage<TestStorageItem> storage = new OddIntRangeDataStorage(2);
    DataStorage<TestStorageItem> secondStorage = new EvenIntRangeDataStorage(2);
    service.registerSearchStorages(
        Map.ofEntries(Map.entry(TEST_STORAGE_1, storage), Map.entry(TEST_STORAGE_2, secondStorage)));
    var request = MultiStorageSearchRequest.<TestSearchItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE_1,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE_1)
                .indexNames(List.of(TEST_INDEX_1, TEST_INDEX_2))
                .storageRequest(BLANK_STORAGE_REQUEST)
                .resultFilter(i -> {
                  if (((TestStorageItem) i).getNumber() == 5) {
                    throw new RuntimeException("wrong number");
                  }
                  return true;
                })
                .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
                .build(),
            TEST_STORAGE_2,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE_2)
                .indexNames(List.of(TEST_INDEX_3, TEST_INDEX_4))
                .storageRequest(BLANK_STORAGE_REQUEST)
                .resultFilter(i -> {
                  if (((TestStorageItem) i).getNumber() == 2) {
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
    searchResult = service.<TestSearchItem>nextSearchResult(searchResult.getSearchId(), 20, "user")
        .toCompletableFuture().join();
    assertEquals(18, searchResult.getResults().size());
    String actual =
        searchResult.getResults().stream().sorted(comparator).map(TestSearchItem::getNumber)
            .collect(Collectors.joining(""));
    String expected = IntStream.range(0, 20).filter(i -> ((i != 5) && (i != 2))).mapToObj(Integer::toString)
        .collect(Collectors.joining(""));
    assertEquals(actual, expected);
  }

  @Test
  void testSearchExceptionInDataSearcher() {
    QdpService service = new QdpService();
    String errorMessage = "Cannot load data";
    DataStorage<TestStorageItem> storage = new DataStorage<>() {
      @Override
      public List<DataSearcher> dataSearcher(RequestStructure storageRequest) {
        return List.of(new DataSearcher() {
          int count;

          @Override
          public List<TestStorageItem> next() {
            if (count == 0) {
              count++;
              return IntStream.range(1, 10)
                  .filter(i -> i % 2 == 1)
                  .mapToObj(TestStorageItem::new)
                  .collect(Collectors.toList());
            } else {
              throw new RuntimeException(errorMessage);
            }
          }

          @Override
          public String getStorageName() {
            return TEST_STORAGE_1;
          }

          @Override
          public List<String> getLibraryIds() {
            return List.of(TEST_INDEX_1);
          }

          @Override
          public void close() {

          }
        });
      }
    };
    DataStorage<TestStorageItem> secondStorage = new EvenIntRangeDataStorage(1);
    service.registerSearchStorages(
        Map.ofEntries(Map.entry(TEST_STORAGE_1, storage), Map.entry(TEST_STORAGE_2, secondStorage)));
    var request = MultiStorageSearchRequest.<TestSearchItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE_1,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE_1)
                .indexNames(List.of(TEST_INDEX_1, TEST_INDEX_2))
                .storageRequest(BLANK_STORAGE_REQUEST)
                .resultFilter(i -> true)
                .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
                .build(),
            TEST_STORAGE_2,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE_2)
                .indexNames(List.of(TEST_INDEX_3, TEST_INDEX_4))
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
    searchResult = service.<TestSearchItem>nextSearchResult(searchResult.getSearchId(), 10, "user")
        .toCompletableFuture().join();
    assertFalse(searchResult.getErrors().isEmpty());
    assertTrue(searchResult.getErrors().get(0).getMessage().contains(errorMessage));

    String actual = searchResult.getResults().stream().sorted(comparator).map(TestSearchItem::getNumber)
        .collect(Collectors.joining(""));
    String expected = IntStream.range(0, 10).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
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
      QdpService[] services = new QdpService[] {new QdpService(system1),
          new QdpService(system2)};
      Thread.sleep(8000);
      DataStorage<TestStorageItem> storage = new OddIntRangeDataStorage(10);
      DataStorage<TestStorageItem> secondStorage = new EvenIntRangeDataStorage(10);
      for (int i = 0; i < 2; i++) {
        services[i].registerSearchStorages(
            Map.ofEntries(Map.entry(TEST_STORAGE_1, storage), Map.entry(TEST_STORAGE_2, secondStorage)));
      }
      var request = MultiStorageSearchRequest.<TestSearchItem>builder()
          .requestStorageMap(Map.of(TEST_STORAGE_1,
              RequestStructure.<TestSearchItem>builder()
                  .storageName(TEST_STORAGE_1)
                  .indexNames(List.of(TEST_INDEX_1, TEST_INDEX_2))
                  .storageRequest(BLANK_STORAGE_REQUEST)
                  .resultFilter(i -> true)
                  .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
                  .build(),
              TEST_STORAGE_2,
              RequestStructure.<TestSearchItem>builder()
                  .storageName(TEST_STORAGE_2)
                  .indexNames(List.of(TEST_INDEX_3, TEST_INDEX_4))
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
      int requestCount = 0;
      SearchResult<TestSearchItem> searchResult = services[0].search(request)
          .thenCompose(sr -> services[0].<TestSearchItem>nextSearchResult(sr.getSearchId(),
              10,
              request.getProcessingSettings().getUser()))
          .toCompletableFuture().join();
      requestCount++;
      assertEquals(10, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      List<TestSearchItem> resultItems = new ArrayList<>(searchResult.getResults());
      for (int i = 0; i < 9; i++) {
        System.out.println("Request " + i);
        searchResult =
            services[requestCount++ % 2].<TestSearchItem>nextSearchResult(searchResult.getSearchId(), 10, "user")
                .toCompletableFuture().join();
        assertEquals(10, searchResult.getResults().size());
        if (i == 8) {
          assertTrue(searchResult.isSearchFinished());
        } else {
          assertFalse(searchResult.isSearchFinished());
        }

        resultItems.addAll(searchResult.getResults());
      }
      String actual =
          resultItems.stream().sorted(comparator).map(TestSearchItem::getNumber).collect(Collectors.joining(""));
      String expected = IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
      assertEquals(expected, actual);
    } finally {
      system1.terminate();
      system2.terminate();
      Await.result(system1.whenTerminated(), Duration.apply(5, TimeUnit.SECONDS));
      Await.result(system2.whenTerminated(), Duration.apply(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void slowStorageTest() {
    QdpService service = new QdpService();

    DataStorage<TestStorageItem> storage = new OddIntRangeDataStorage(10);
    DataStorage<TestStorageItem> secondStorage = new SlowEvenIntRangeDataStorage(10);
    service.registerSearchStorages(
        Map.ofEntries(Map.entry(TEST_STORAGE_1, storage), Map.entry(TEST_STORAGE_2, secondStorage)));
    var request = MultiStorageSearchRequest.<TestSearchItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE_1,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE_1)
                .indexNames(List.of(TEST_INDEX_1, TEST_INDEX_2))
                .storageRequest(BLANK_STORAGE_REQUEST)
                .resultFilter(i -> true)
                .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
                .build(),
            TEST_STORAGE_2,
            RequestStructure.<TestSearchItem>builder()
                .storageName(TEST_STORAGE_2)
                .indexNames(List.of(TEST_INDEX_3, TEST_INDEX_4))
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
    String actual =
        resultItems.stream().sorted(comparator).map(TestSearchItem::getNumber).collect(Collectors.joining(""));
    String expected = IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
  }

  public static class OddIntRangeDataStorage implements DataStorage<TestStorageItem> {

    private final int chunks;

    public OddIntRangeDataStorage(int chunks) {
      this.chunks = chunks;
    }

    @Override
    public List<DataSearcher> dataSearcher(RequestStructure storageRequest) {
      return List.of(new DataSearcher() {
        int counter;

        @Override
        public List<? extends StorageItem> next() {
          if (counter >= chunks) {
            return List.of();
          } else {
            List<StorageItem> result =
                IntStream.range(counter * 10, (counter + 1) * 10)
                    .filter(i -> i % 2 == 1)
                    .mapToObj(TestStorageItem::new)
                    .collect(Collectors.toList());
            counter++;
            return result;
          }
        }

        @Override
        public String getStorageName() {
          return TEST_STORAGE_1;
        }

        @Override
        public List<String> getLibraryIds() {
          return List.of(TEST_INDEX_1, TEST_INDEX_2);
        }

        @Override
        public void close() throws Exception {

        }
      });
    }
  }

  public static class EvenIntRangeDataStorage implements DataStorage<TestStorageItem> {

    private final int chunks;

    public EvenIntRangeDataStorage(int chunks) {
      this.chunks = chunks;
    }

    @Override
    public List<DataSearcher> dataSearcher(RequestStructure storageRequest) {
      return List.of(new DataSearcher() {
        int counter;

        @Override
        public List<? extends StorageItem> next() {
          if (counter >= chunks) {
            return List.of();
          } else {
            List<StorageItem> result =
                IntStream.range(counter * 10, (counter + 1) * 10)
                    .filter(i -> i % 2 == 0)
                    .mapToObj(TestStorageItem::new)
                    .collect(Collectors.toList());
            counter++;
            return result;
          }
        }

        @Override
        public String getStorageName() {
          return TEST_STORAGE_2;
        }

        @Override
        public List<String> getLibraryIds() {
          return List.of(TEST_INDEX_3, TEST_INDEX_4);
        }

        @Override
        public void close() throws Exception {

        }
      });
    }
  }

  public static class SlowEvenIntRangeDataStorage implements DataStorage<TestStorageItem> {

    private final int chunks;

    public SlowEvenIntRangeDataStorage(int chunks) {
      this.chunks = chunks;
    }

    @Override
    public List<DataSearcher> dataSearcher(RequestStructure storageRequest) {
      return List.of(new DataSearcher() {
        int counter;

        @Override
        public List<? extends StorageItem> next() {
          if (counter >= chunks) {
            return List.of();
          } else {
            List<StorageItem> result =
                IntStream.range(counter * 10, (counter + 1) * 10)
                    .filter(i -> i % 2 == 0)
                    .mapToObj(TestStorageItem::new)
                    .peek(i -> {
                      try {
                        Thread.sleep(150);
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                      }
                    })
                    .collect(Collectors.toList());
            counter++;
            return result;
          }
        }

        @Override
        public String getStorageName() {
          return TEST_STORAGE_2;
        }

        @Override
        public List<String> getLibraryIds() {
          return List.of(TEST_INDEX_3, TEST_INDEX_4);
        }

        @Override
        public void close() throws Exception {

        }
      });
    }
  }
}
