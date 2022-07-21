package com.quantori.qdp.core.source;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.pattern.StatusReply;
import com.quantori.qdp.core.source.model.DataLibrary;
import com.quantori.qdp.core.source.model.DataLoader;
import com.quantori.qdp.core.source.model.DataSearcher;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.MultiStorageSearchRequest;
import com.quantori.qdp.core.source.model.ProcessingSettings;
import com.quantori.qdp.core.source.model.RequestStructure;
import com.quantori.qdp.core.source.model.SearchResult;
import com.quantori.qdp.core.source.model.SearchStrategy;
import com.quantori.qdp.core.source.model.StorageItem;
import com.quantori.qdp.core.source.model.StorageRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SearchFlowBufferTest {
  public static final String TEST_STORAGE = "testStorage";
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
    var filter = getFilterFunction();
    var transformer = getTransformFunction();
    var storageRequest = new StorageRequest() {
    };
    var request = SearchRequest.builder()
        .requestStructure(RequestStructure.<Molecule>builder()
            .storageName(TEST_STORAGE)
            .indexNames(List.of("testIndex"))
            .storageRequest(storageRequest)
            .resultFilter(filter)
            .resultTransformer(transformer)
            .build())
        .processingSettings(ProcessingSettings.builder()
            .user("user")
            .hardLimit(1)
            .pageSize(1)
            .strategy(SearchStrategy.PAGE_FROM_STREAM)
            .bufferSize(BUFFER_SIZE)
            .build())
        .build();
    List<Molecule> result = getSearchItems(batches, cdl, request);
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
    var filter = getFilterFunction();
    var transformer = getTransformFunction();
    var storageRequest = new StorageRequest() {
    };
    var request = SearchRequest.builder()
        .requestStructure(RequestStructure.<Molecule>builder()
            .storageName(TEST_STORAGE)
            .indexNames(List.of("testIndex"))
            .storageRequest(storageRequest)
            .resultFilter(filter)
            .resultTransformer(transformer)
            .build())
        .processingSettings(ProcessingSettings.builder()
            .user("user")
            .hardLimit(1)
            .pageSize(1)
            .strategy(SearchStrategy.PAGE_FROM_STREAM)
            .bufferSize(BUFFER_SIZE)
            .parallelism(2)
            .build())
        .build();
    List<Molecule> result = getSearchItems(batches, cdl, request);
    Assertions.assertEquals(1, result.size());

    boolean resultSuccess = false;
    try {
      resultSuccess = cdl.await(2, TimeUnit.SECONDS);
    } catch (Exception ignored) {
    }
    log.info("cdl count: " + cdl.getCount());
    Assertions.assertTrue(resultSuccess);
  }

  private List<Molecule> getSearchItems(
      List<List<StorageItem>> batches, CountDownLatch cdl, SearchRequest request)
      throws InterruptedException {
    String name = UUID.randomUUID().toString();
    ActorRef<SearchActor.Command> toSearch = testKit.spawn(
        SearchActor.create(name, Map.of(request.getRequestStructure().getStorageName(), getStorage(batches, cdl)))
    );
    TestProbe<StatusReply<SearchResult<Molecule>>> probe = testKit.createTestProbe();
    toSearch.tell(new SearchActor.Search<>(probe.ref(), MultiStorageSearchRequest.<Molecule>builder()
        .requestStorageMap(Map.of(TEST_STORAGE, request.getRequestStructure()))
        .processingSettings(request.getProcessingSettings())
        .build()));

    var result = probe.receiveMessage(Duration.ofSeconds(10)).getValue();
    List<Molecule> list = new ArrayList<>(result.getResults());
    int count = 0;
    while (!result.isSearchFinished() && list.size() < request.getProcessingSettings().getPageSize() && count < 10) {
      Thread.sleep(1000);
      toSearch.tell(new SearchActor.SearchNext<>(probe.ref(), request.getProcessingSettings().getPageSize(), "user"));
      result = probe.receiveMessage(Duration.ofSeconds(10)).getValue();
      list.addAll(result.getResults());
      count++;
    }

    return list;
  }

  private DataStorage<StorageItem> getStorage(List<List<StorageItem>> batches, CountDownLatch cdl) {
    return new DataStorage<>() {
      @Override
      public DataLoader<StorageItem> dataLoader(String indexName) {
        return null;
      }

      @Override
      public List<DataLibrary> getLibraries() {
        return List.of();
      }

      @Override
      public DataLibrary createLibrary(DataLibrary index) {
        return null;
      }

      @Override
      public DataSearcher dataSearcher(StorageRequest storageRequest) {
        return getDataSearcher(batches, cdl);
      }
    };
  }


  private List<List<StorageItem>> getBatches(int batch, int total) {
    List<List<StorageItem>> batches = new ArrayList<>();
    List<StorageItem> batchArray = new ArrayList<>();
    int totalCount = 0;
    while (total > totalCount) {
      var next = new StorageItem() {
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

  private DataSearcher getDataSearcher(List<List<StorageItem>> batches, CountDownLatch cdl) {
    return new DataSearcher() {
      final Iterator<List<StorageItem>> iterator = batches.iterator();

      @Override
      public List<? extends StorageItem> next() {
        List<StorageItem> list = iterator.hasNext() ? iterator.next() : List.of();
        var it = list.iterator();
        log.debug("dataSearcher was asked for next {} items", list.size());
        return new ArrayList<>(list) {
          public Iterator<StorageItem> iterator() {
            return new Iterator<>() {
              @Override
              public boolean hasNext() {
                return it.hasNext();
              }

              @Override
              public StorageItem next() {
                cdl.countDown();
                return it.next();
              }
            };
          }
        };
      }

      @Override
      public void close() {
      }
    };
  }

  private Predicate<StorageItem> getFilterFunction() {
    return storageResultItem -> true;
  }

  private Function<StorageItem, Molecule> getTransformFunction() {
    return storageResultItem -> new Molecule() {
      public String toString() {
        return storageResultItem.toString();
      }
    };
  }
}