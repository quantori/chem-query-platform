package com.quantori.qdp.core.source;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.ActorRef;
import akka.pattern.StatusReply;
import com.quantori.qdp.api.model.core.DataLoader;
import com.quantori.qdp.api.model.core.DataSearcher;
import com.quantori.qdp.api.model.core.DataStorage;
import com.quantori.qdp.api.model.core.MultiStorageSearchRequest;
import com.quantori.qdp.api.model.core.ProcessingSettings;
import com.quantori.qdp.api.model.core.RequestStructure;
import com.quantori.qdp.api.model.core.SearchResult;
import com.quantori.qdp.api.model.core.StorageRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SearchFlowBufferTest {
  public static final String TEST_STORAGE = "testStorage";
  public static final String TEST_INDEX = "testIndex";
  Logger log = LoggerFactory.getLogger(SearchFlowBufferTest.class);
  static final ActorTestKit testKit = ActorTestKit.create();

  @AfterAll
  public static void teardown() {
    testKit.shutdownTestKit();
  }

  @Test
  void bufferSize8Parallelism1() throws InterruptedException {

    final int BATCH = 3;
    final int BUFFER_SIZE = 8;
    CountDownLatch cdl = new CountDownLatch(((BUFFER_SIZE / BATCH) + 2) * BATCH);
    log.debug("Count: {}", cdl.getCount());

    var batches = getBatches(BATCH, 200);
    var storageRequest = new StorageRequest() {
    };
    var request = SearchRequest.builder()
        .requestStructure(RequestStructure.<TestSearchItem, TestStorageItem>builder()
            .storageName(TEST_STORAGE)
            .indexNames(List.of(TEST_INDEX))
            .storageRequest(storageRequest)
            .resultFilter(item -> true)
            .resultTransformer(item -> new TestSearchItem(item.getId()))
            .build())
        .processingSettings(ProcessingSettings.builder()
            .user("user")
            .bufferSize(BUFFER_SIZE)
            .build())
        .build();
    List<TestSearchItem> result = getStorageItems(batches, cdl, request);
    Assertions.assertEquals(1, result.size());

    boolean resultSuccess = false;
    try {
      resultSuccess = cdl.await(2, TimeUnit.SECONDS);
    } catch (Exception ignored) {
    }
    log.info("cdl count: " + cdl.getCount());
    Assertions.assertTrue(resultSuccess);
  }

  @Test
  void bufferSize8Parallelism2() throws InterruptedException {

    final int BATCH = 5;
    final int BUFFER_SIZE = 16;
    CountDownLatch cdl = new CountDownLatch(((BUFFER_SIZE / BATCH) + 2) * BATCH);
    log.debug("Count: {}", cdl.getCount());

    var batches = getBatches(BATCH, 100);
    var storageRequest = new StorageRequest() {
    };
    var request = SearchRequest.builder()
        .requestStructure(RequestStructure.<TestSearchItem, TestStorageItem>builder()
            .storageName(TEST_STORAGE)
            .indexNames(List.of(TEST_INDEX))
            .storageRequest(storageRequest)
            .resultFilter(item -> true)
            .resultTransformer(item -> new TestSearchItem(item.getId()))
            .build())
        .processingSettings(ProcessingSettings.builder()
            .user("user")
            .bufferSize(BUFFER_SIZE)
            .parallelism(2)
            .build())
        .build();
    List<TestSearchItem> result = getStorageItems(batches, cdl, request);
    Assertions.assertEquals(1, result.size());

    boolean resultSuccess = false;
    try {
      resultSuccess = cdl.await(2, TimeUnit.SECONDS);
    } catch (Exception ignored) {
    }
    log.info("cdl count: " + cdl.getCount());
    Assertions.assertTrue(resultSuccess);
  }

  private List<TestSearchItem> getStorageItems(
      List<List<TestStorageItem>> batches, CountDownLatch cdl, SearchRequest request)
      throws InterruptedException {
    String name = UUID.randomUUID().toString();
    ActorRef<SearchActor.Command> toSearch = testKit.spawn(
        SearchActor.create(name, Map.of(request.getRequestStructure().getStorageName(), getStorage(batches, cdl)))
    );
    var probe = testKit.<StatusReply<SearchResult<TestSearchItem>>>createTestProbe();
    toSearch.tell(
        new SearchActor.Search<>(probe.ref(), MultiStorageSearchRequest.<TestSearchItem, TestStorageItem>builder()
            .requestStorageMap(Map.of(TEST_STORAGE, request.getRequestStructure()))
            .processingSettings(request.getProcessingSettings())
            .build()));

    var result = probe.receiveMessage(Duration.ofSeconds(10)).getValue();
    List<TestSearchItem> list = new ArrayList<>(result.getResults());
    int count = 0;
    while (!result.isSearchFinished() && list.size() < 1 && count < 10) {
      Thread.sleep(1000);
      toSearch.tell(new SearchActor.SearchNext<>(probe.ref(), 1, "user"));
      result = probe.receiveMessage(Duration.ofSeconds(10)).getValue();
      list.addAll(result.getResults());
      count++;
    }

    return list;
  }

  private DataStorage<TestStorageUploadItem, TestSearchItem, TestStorageItem> getStorage(
      List<List<TestStorageItem>> batches, CountDownLatch cdl) {
    return new DataStorage<>() {
      @Override
      public DataLoader<TestStorageUploadItem> dataLoader(String libraryId) {
        return null;
      }

      @Override
      public List<DataSearcher<TestStorageItem>> dataSearcher(
          RequestStructure<TestSearchItem, TestStorageItem> storageRequest) {
        return List.of(getDataSearcher(batches, cdl));
      }
    };
  }

  private List<List<TestStorageItem>> getBatches(int batch, int total) {
    List<List<TestStorageItem>> batches = new ArrayList<>();
    List<TestStorageItem> batchArray = new ArrayList<>();
    int totalCount = 0;
    while (total > totalCount) {
      var next = new TestStorageItem() {
        final int number = batches.size() * batch + batchArray.size();

        @Override
        public String toString() {
          return " {" +
              "num=" + number +
              '}';
        }
      };
      batchArray.add(next);
      totalCount++;
      if (batchArray.size() == batch) {
        batches.add(new ArrayList<>(batchArray));
        batchArray.clear();
      }
    }
    batches.add(batchArray);
    return batches;
  }

  private DataSearcher<TestStorageItem> getDataSearcher(List<List<TestStorageItem>> batches, CountDownLatch cdl) {
    return new DataSearcher<>() {
      final Iterator<List<TestStorageItem>> iterator = batches.iterator();

      @Override
      public List<TestStorageItem> next() {
        List<TestStorageItem> list = iterator.hasNext() ? iterator.next() : List.of();
        var it = list.iterator();
        log.debug("dataSearcher was asked for next {} items", list.size());
        return new ArrayList<>(list) {
          public Iterator<TestStorageItem> iterator() {
            return new Iterator<>() {
              @Override
              public boolean hasNext() {
                return it.hasNext();
              }

              @Override
              public TestStorageItem next() {
                cdl.countDown();
                return it.next();
              }
            };
          }
        };
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
    };
  }

}