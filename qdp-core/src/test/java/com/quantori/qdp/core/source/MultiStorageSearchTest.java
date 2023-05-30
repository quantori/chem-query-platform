package com.quantori.qdp.core.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.actor.typed.ActorSystem;
import com.quantori.qdp.api.model.core.DataStorage;
import com.quantori.qdp.api.model.core.SearchRequest;
import com.quantori.qdp.api.model.core.SearchResult;
import com.quantori.qdp.api.model.core.StorageRequest;
import com.quantori.qdp.api.service.ItemWriter;
import com.quantori.qdp.api.service.SearchIterator;
import com.quantori.qdp.core.configuration.ClusterConfigurationProperties;
import com.quantori.qdp.core.configuration.ClusterProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;


class MultiStorageSearchTest {
  private static final String TEST_STORAGE_1 = "test_storage_1";
  private static final String TEST_STORAGE_2 = "test_storage_2";
  public static final Function<TestStorageItem, TestSearchItem>
      RESULT_ITEM_NUMBER_FUNCTION = item -> new TestSearchItem(item.getId());
  public static final Comparator<TestSearchItem> comparator =
      Comparator.comparingInt(o -> Integer.parseInt(o.getId()));
  public static final String TEST_INDEX_1 = "testIndex1";
  public static final String TEST_INDEX_2 = "testIndex2";
  public static final String TEST_INDEX_3 = "testIndex3";
  public static final String TEST_INDEX_4 = "testIndex4";

  @Test
  void testSearch() {
    var storage = new OddIntRangeDataStorage(10);
    var secondStorage = new EvenIntRangeDataStorage(10);
    QdpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new QdpService<>(Map.ofEntries(Map.entry(TEST_STORAGE_1, storage), Map.entry(TEST_STORAGE_2, secondStorage)));

    var request = SearchRequest.<TestSearchItem, TestStorageItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE_1,
            StorageRequest.builder()
                .storageName(TEST_STORAGE_1)
                .indexIds(List.of(TEST_INDEX_1, TEST_INDEX_2))
                .build(),
            TEST_STORAGE_2,
            StorageRequest.builder()
                .storageName(TEST_STORAGE_2)
                .indexIds(List.of(TEST_INDEX_3, TEST_INDEX_4))
                .build()))
        .user("user")
        .bufferSize(100)
        .fetchLimit(100)
        .parallelism(1)
        .resultFilter(i -> true)
        .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
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
    String actual =
        resultItems.stream().sorted(comparator).map(TestSearchItem::getId).collect(Collectors.joining(""));
    String expected = IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
  }

  @Test
  void testSearchInLoop() {
    var storage = new OddIntRangeDataStorage(10);
    var secondStorage = new EvenIntRangeDataStorage(10);
    QdpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new QdpService<>(Map.ofEntries(Map.entry(TEST_STORAGE_1, storage), Map.entry(TEST_STORAGE_2, secondStorage)));
    var request = SearchRequest.<TestSearchItem, TestStorageItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE_1,
            StorageRequest.builder()
                .storageName(TEST_STORAGE_1)
                .indexIds(List.of(TEST_INDEX_1, TEST_INDEX_2))
                .build(),
            TEST_STORAGE_2,
            StorageRequest.builder()
                .storageName(TEST_STORAGE_2)
                .indexIds(List.of(TEST_INDEX_3, TEST_INDEX_4))
                .build()))
        .user("user")
        .bufferSize(100)
        .fetchLimit(100)
        .parallelism(1)
        .resultFilter(i -> true)
        .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
        .build();
    SearchResult<TestSearchItem> searchResult = service.search(request).toCompletableFuture().join();
    List<TestSearchItem> resultItems = new ArrayList<>(searchResult.getResults());
    while (!searchResult.isSearchFinished()) {
      searchResult = service.nextSearchResult(searchResult.getSearchId(), 8, "user")
          .toCompletableFuture().join();
      resultItems.addAll(searchResult.getResults());
    }

    String actual =
        resultItems.stream().sorted(comparator).map(TestSearchItem::getId).collect(Collectors.joining(""));
    String expected = IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
  }

  @Test
  void testSearchExceptionInTransformer() {
    var storage = new OddIntRangeDataStorage(2);
    var secondStorage = new EvenIntRangeDataStorage(2);
    QdpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new QdpService<>(Map.ofEntries(Map.entry(TEST_STORAGE_1, storage), Map.entry(TEST_STORAGE_2, secondStorage)));
    var request = SearchRequest.<TestSearchItem, TestStorageItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE_1,
            StorageRequest.builder()
                .storageName(TEST_STORAGE_1)
                .indexIds(List.of(TEST_INDEX_1, TEST_INDEX_2))
                .build(),
            TEST_STORAGE_2,
            StorageRequest.builder()
                .storageName(TEST_STORAGE_2)
                .indexIds(List.of(TEST_INDEX_3, TEST_INDEX_4))
                .build()))
        .user("user")
        .bufferSize(100)
        .fetchLimit(100)
        .parallelism(1)
        .resultFilter(i -> true)
        .resultTransformer(item -> {
          if (Set.of("2", "5").contains(item.getId())) {
            throw new RuntimeException("wrong number");
          }
          return new TestSearchItem(item.getId());
        })
        .build();

    SearchResult<TestSearchItem> searchResult = service.search(request).toCompletableFuture().join();
    searchResult = service.nextSearchResult(searchResult.getSearchId(), 20, "user")
        .toCompletableFuture().join();
    assertEquals(18, searchResult.getResults().size());
    String actual =
        searchResult.getResults().stream().sorted(comparator).map(TestSearchItem::getId)
            .collect(Collectors.joining(""));
    String expected = IntStream.range(0, 20).filter(i -> ((i != 5) && (i != 2))).mapToObj(Integer::toString)
        .collect(Collectors.joining(""));
    assertEquals(actual, expected);
  }

  @Test
  void testSearchExceptionInFilter() {
    var storage = new OddIntRangeDataStorage(2);
    var secondStorage = new EvenIntRangeDataStorage(2);
    QdpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new QdpService<>(Map.ofEntries(Map.entry(TEST_STORAGE_1, storage), Map.entry(TEST_STORAGE_2, secondStorage)));
    var request = SearchRequest.<TestSearchItem, TestStorageItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE_1,
            StorageRequest.builder()
                .storageName(TEST_STORAGE_1)
                .indexIds(List.of(TEST_INDEX_1, TEST_INDEX_2))
                .build(),
            TEST_STORAGE_2,
            StorageRequest.builder()
                .storageName(TEST_STORAGE_2)
                .indexIds(List.of(TEST_INDEX_3, TEST_INDEX_4))
                .build()))
        .user("user")
        .bufferSize(100)
        .fetchLimit(100)
        .parallelism(1)
        .resultFilter(item -> {
          if (Set.of("2", "5").contains(item.getId())) {
            throw new RuntimeException("wrong number");
          }
          return true;
        })
        .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
        .build();

    SearchResult<TestSearchItem> searchResult = service.search(request).toCompletableFuture().join();
    searchResult = service.nextSearchResult(searchResult.getSearchId(), 20, "user")
        .toCompletableFuture().join();
    assertEquals(18, searchResult.getResults().size());
    String actual =
        searchResult.getResults().stream().sorted(comparator).map(TestSearchItem::getId)
            .collect(Collectors.joining(""));
    String expected = IntStream.range(0, 20).filter(i -> ((i != 5) && (i != 2))).mapToObj(Integer::toString)
        .collect(Collectors.joining(""));
    assertEquals(actual, expected);
  }

  @Test
  void testSearchExceptionInDataSearcher() {
    String errorMessage = "Cannot load data";
    DataStorage<TestStorageUploadItem, TestStorageItem> storage = new DataStorage<>() {
      @Override
      public ItemWriter<TestStorageUploadItem> itemWriter(String libraryId) {
        return null;
      }

      @Override
      public List<SearchIterator<TestStorageItem>> searchIterator(StorageRequest storageRequest) {
        return List.of(new SearchIterator<>() {
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
    DataStorage<TestStorageUploadItem, TestStorageItem> secondStorage = new EvenIntRangeDataStorage(1);
    QdpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new QdpService<>(Map.ofEntries(Map.entry(TEST_STORAGE_1, storage), Map.entry(TEST_STORAGE_2, secondStorage)));
    var request = SearchRequest.<TestSearchItem, TestStorageItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE_1,
            StorageRequest.builder()
                .storageName(TEST_STORAGE_1)
                .indexIds(List.of(TEST_INDEX_1, TEST_INDEX_2))
                .build(),
            TEST_STORAGE_2,
            StorageRequest.builder()
                .storageName(TEST_STORAGE_2)
                .indexIds(List.of(TEST_INDEX_3, TEST_INDEX_4))
                .build()))
        .user("user")
        .bufferSize(100)
        .fetchLimit(100)
        .parallelism(1)
        .resultFilter(i -> true)
        .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
        .build();
    SearchResult<TestSearchItem> searchResult = service.search(request).toCompletableFuture().join();
    searchResult = service.nextSearchResult(searchResult.getSearchId(), 10, "user")
        .toCompletableFuture().join();
    assertThat(searchResult.getErrors()).isNotEmpty();
    assertThat(searchResult.getErrors().get(0).getMessage()).contains(errorMessage);

    String actual = searchResult.getResults().stream().sorted(comparator).map(TestSearchItem::getId)
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
      Thread.sleep(8000);
      DataStorage<TestStorageUploadItem, TestStorageItem> storage = new OddIntRangeDataStorage(10);
      DataStorage<TestStorageUploadItem, TestStorageItem> secondStorage = new EvenIntRangeDataStorage(10);
      List<QdpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem>> services =
          List.of(new QdpService<>(Map.ofEntries(Map.entry(TEST_STORAGE_1, storage),
                  Map.entry(TEST_STORAGE_2, secondStorage)), 10, system1),
              new QdpService<>(Map.ofEntries(Map.entry(TEST_STORAGE_1, storage),
                  Map.entry(TEST_STORAGE_2, secondStorage)), 10, system2));
      var request = SearchRequest.<TestSearchItem, TestStorageItem>builder()
          .requestStorageMap(Map.of(TEST_STORAGE_1,
              StorageRequest.builder()
                  .storageName(TEST_STORAGE_1)
                  .indexIds(List.of(TEST_INDEX_1, TEST_INDEX_2))
                  .build(),
              TEST_STORAGE_2,
              StorageRequest.builder()
                  .storageName(TEST_STORAGE_2)
                  .indexIds(List.of(TEST_INDEX_3, TEST_INDEX_4))
                  .build()))
          .user("user")
          .bufferSize(100)
          .fetchLimit(100)
          .parallelism(1)
          .resultFilter(i -> true)
          .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
          .build();
      int requestCount = 0;
      SearchResult<TestSearchItem> searchResult = services.get(0).search(request)
          .thenCompose(sr -> services.get(0).nextSearchResult(sr.getSearchId(), 10,
              request.getUser()))
          .toCompletableFuture().join();
      requestCount++;
      assertEquals(10, searchResult.getResults().size());
      assertFalse(searchResult.isSearchFinished());
      List<TestSearchItem> resultItems = new ArrayList<>(searchResult.getResults());
      for (int i = 0; i < 9; i++) {
        System.out.println("Request " + i);
        searchResult = services.get(requestCount++ % 2).nextSearchResult(searchResult.getSearchId(), 10, "user")
            .toCompletableFuture().join();
        assertEquals(10, searchResult.getResults().size());
        if (i == 8) {
          assertTrue(searchResult.isSearchFinished());
        } else {
          assertFalse(searchResult.isSearchFinished());
        }

        resultItems.addAll(searchResult.getResults());
      }
      String actual = resultItems.stream()
          .sorted(comparator).map(TestSearchItem::getId).collect(Collectors.joining(""));
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
    DataStorage<TestStorageUploadItem, TestStorageItem> storage = new OddIntRangeDataStorage(10);
    DataStorage<TestStorageUploadItem, TestStorageItem> secondStorage = new SlowEvenIntRangeDataStorage(10);
    QdpService<TestDataUploadItem, TestStorageUploadItem, TestSearchItem, TestStorageItem> service =
        new QdpService<>(Map.ofEntries(Map.entry(TEST_STORAGE_1, storage), Map.entry(TEST_STORAGE_2, secondStorage)));
    var request = SearchRequest.<TestSearchItem, TestStorageItem>builder()
        .requestStorageMap(Map.of(TEST_STORAGE_1,
            StorageRequest.builder()
                .storageName(TEST_STORAGE_1)
                .indexIds(List.of(TEST_INDEX_1, TEST_INDEX_2))
                .build(),
            TEST_STORAGE_2,
            StorageRequest.builder()
                .storageName(TEST_STORAGE_2)
                .indexIds(List.of(TEST_INDEX_3, TEST_INDEX_4))
                .build()))
        .user("user")
        .bufferSize(100)
        .fetchLimit(100)
        .parallelism(1)
        .resultFilter(i -> true)
        .resultTransformer(RESULT_ITEM_NUMBER_FUNCTION)
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
    String actual =
        resultItems.stream().sorted(comparator).map(TestSearchItem::getId).collect(Collectors.joining(""));
    String expected = IntStream.range(0, 100).mapToObj(Integer::toString).collect(Collectors.joining(""));
    assertEquals(expected, actual);
  }

  public static class OddIntRangeDataStorage implements DataStorage<TestStorageUploadItem, TestStorageItem> {

    private final int chunks;

    public OddIntRangeDataStorage(int chunks) {
      this.chunks = chunks;
    }

    @Override
    public ItemWriter<TestStorageUploadItem> itemWriter(String libraryId) {
      return null;
    }

    @Override
    public List<SearchIterator<TestStorageItem>> searchIterator(StorageRequest storageRequest) {
      return List.of(new SearchIterator<>() {
        int counter;

        @Override
        public List<TestStorageItem> next() {
          if (counter >= chunks) {
            return List.of();
          } else {
            List<TestStorageItem> result =
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
        public void close() {

        }
      });
    }
  }

  public static class EvenIntRangeDataStorage implements DataStorage<TestStorageUploadItem, TestStorageItem> {

    private final int chunks;

    public EvenIntRangeDataStorage(int chunks) {
      this.chunks = chunks;
    }

    @Override
    public ItemWriter<TestStorageUploadItem> itemWriter(String libraryId) {
      return null;
    }

    @Override
    public List<SearchIterator<TestStorageItem>> searchIterator(StorageRequest storageRequest) {
      return List.of(new SearchIterator<>() {
        int counter;

        @Override
        public List<TestStorageItem> next() {
          if (counter >= chunks) {
            return List.of();
          } else {
            List<TestStorageItem> result =
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
        public void close() {

        }
      });
    }
  }

  public static class SlowEvenIntRangeDataStorage implements DataStorage<TestStorageUploadItem, TestStorageItem> {

    private final int chunks;

    public SlowEvenIntRangeDataStorage(int chunks) {
      this.chunks = chunks;
    }

    @Override
    public ItemWriter<TestStorageUploadItem> itemWriter(String libraryId) {
      return null;
    }

    @Override
    public List<SearchIterator<TestStorageItem>> searchIterator(StorageRequest storageRequest) {
      return List.of(new SearchIterator<>() {
        int counter;

        @Override
        public List<TestStorageItem> next() {
          if (counter >= chunks) {
            return List.of();
          } else {
            List<TestStorageItem> result =
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
        public void close() {

        }
      });
    }
  }
}
