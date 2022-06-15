package com.quantori.qdp.core.source;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.pattern.StatusReply;
import com.quantori.qdp.core.source.external.ExternalSearchActor;
import com.quantori.qdp.core.source.model.DataLibrary;
import com.quantori.qdp.core.source.model.DataLoader;
import com.quantori.qdp.core.source.model.DataSearcher;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.molecule.Molecule;
import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import com.quantori.qdp.core.source.model.molecule.search.SearchResult;
import com.quantori.qdp.core.source.model.molecule.search.SearchResultItem;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
    var storageRequest = new SearchRequest.Request() { };
    SearchRequest request = SearchRequest.builder()
        .storageName("testStorage")
        .indexNames(List.of("testIndex"))
        .hardLimit(1)
        .pageSize(1)
        .strategy(SearchRequest.SearchStrategy.PAGE_FROM_STREAM)
        .storageRequest(storageRequest)
        .resultFilter(filter)
        .resultTransformer(transformer)
        .bufferSize(BUFFER_SIZE)
        .build();
    List<? extends SearchResultItem> result = getSearchResultItems(batches, cdl, request);
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
    var storageRequest = new SearchRequest.Request() {};

    SearchRequest request = SearchRequest.builder()
        .storageName("testStorage")
        .indexNames(List.of("testIndex"))
        .hardLimit(1)
        .pageSize(1)
        .strategy(SearchRequest.SearchStrategy.PAGE_FROM_STREAM)
        .storageRequest(storageRequest)
        .resultFilter(filter)
        .resultTransformer(transformer)
        .bufferSize(BUFFER_SIZE)
        .parallelism(2)
        .build();
    List<? extends SearchResultItem> result = getSearchResultItems(batches, cdl, request);
    Assertions.assertEquals(1, result.size());

    boolean resultSuccess = false;
    try {
      resultSuccess = cdl.await(2, TimeUnit.SECONDS);
    } catch (Exception ignored) {
    }
    log.info("cdl count: " + cdl.getCount());
    Assertions.assertTrue(resultSuccess);
  }

  private List<? extends SearchResultItem> getSearchResultItems(
      List<List<SearchRequest.StorageResultItem>> batches, CountDownLatch cdl, SearchRequest request)
      throws InterruptedException {
    String name = UUID.randomUUID().toString();
    ActorRef<MoleculeSearchActor.Command> toSearch = testKit.spawn(
        ExternalSearchActor.create(request.getStorageName(), getStorage(batches, cdl)), name
    );
    TestProbe<StatusReply<SearchResult>> probe = testKit.createTestProbe();
    toSearch.tell(new MoleculeSearchActor.Search(probe.ref(), request));

    var result = probe.receiveMessage(Duration.ofSeconds(10)).getValue();
    List<SearchResultItem> list = new ArrayList<>(result.getResults());
    int count = 0;
    while (!result.isSearchFinished() && list.size() < request.getPageSize() && count < 10){
      Thread.sleep(1000);
      toSearch.tell(new MoleculeSearchActor.SearchNext(probe.ref(), request.getPageSize(),"user"));
      result = probe.receiveMessage(Duration.ofSeconds(10)).getValue();
      list.addAll(result.getResults());
      count++;
    }

    return list;
  }

  private DataStorage<? extends Molecule> getStorage(
      List<List<SearchRequest.StorageResultItem>> batches, CountDownLatch cdl) {
    return new DataStorage<>() {
      @Override
      public DataLoader<Molecule> dataLoader(String indexName) {
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
      public DataSearcher dataSearcher(SearchRequest searchRequest) {
        return getDataSearcher(batches, cdl);
      }
    };
  }


  private List<List<SearchRequest.StorageResultItem>> getBatches(int batch, int total) {
    List<List<SearchRequest.StorageResultItem>> batches = new ArrayList<>();
    List<SearchRequest.StorageResultItem> batchArray = new ArrayList<>();
    int totalCount = 0;
    while (total > totalCount) {
      var next = new SearchRequest.StorageResultItem() {
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

  private DataSearcher getDataSearcher(List<List<SearchRequest.StorageResultItem>> batches, CountDownLatch cdl) {
    return new DataSearcher() {
      final Iterator<List<SearchRequest.StorageResultItem>> iterator = batches.iterator();

      @Override
      public List<? extends SearchRequest.StorageResultItem> next() {
        List<SearchRequest.StorageResultItem> list = iterator.hasNext() ? iterator.next() : List.of();
        var it = list.iterator();
        log.debug("dataSearcher was asked for next {} items", list.size());
        return new ArrayList<>(list) {
          public Iterator<SearchRequest.StorageResultItem> iterator() {
            return new Iterator<>() {
              @Override
              public boolean hasNext() {
                return it.hasNext();
              }

              @Override
              public SearchRequest.StorageResultItem next() {
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

  private Predicate<SearchRequest.StorageResultItem> getFilterFunction() {
    return storageResultItem -> true;
  }

  private Function<SearchRequest.StorageResultItem, SearchResultItem> getTransformFunction() {
    return storageResultItem -> new SearchResultItem() {
      public String toString() {
        return storageResultItem.toString();
      }
    };
  }
}