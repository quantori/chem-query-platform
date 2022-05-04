package com.quantori.qdp.core.source.external;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.testkit.typed.javadsl.TestProbe;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.pattern.StatusReply;
import com.quantori.qdp.core.source.MoleculeSearchActor;
import com.quantori.qdp.core.source.model.DataSearcher;
import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import com.quantori.qdp.core.source.model.molecule.search.SearchResult;
import com.quantori.qdp.core.source.model.molecule.search.SearchResultItem;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SearcherTest {

  static final ActorTestKit testKit = ActorTestKit.create();

  @AfterAll
  static void teardown() {
    testKit.shutdownTestKit();
  }

  static Stream<Arguments> testSearchers() {
    return Stream.of(
        Arguments.of(SearchRequest.SearchStrategy.PAGE_FROM_STREAM),
        Arguments.of(SearchRequest.SearchStrategy.PAGE_BY_PAGE)
    );
  }

  @ParameterizedTest(name = "searchFromStream ({arguments})")
  @MethodSource("testSearchers")
  void searchFromStream(SearchRequest.SearchStrategy strategy) {
    var storageRequest = testStorageRequest();
    var filter = getFilterFunction();
    var transformer = getTransformFunction();

    var request = new SearchRequest.Builder()
        .storageName("testStorage")
        .indexName("testIndex")
        .hardLimit(10)
        .pageSize(10)
        .strategy(strategy)
        .request(storageRequest)
        .resultFilter(filter)
        .resultTransformer(transformer)
        .bufferSize(8)
        .parallelism(3)
        .build();


    var batches = getBatches(3, 10);
    var dataSearcher = getQdpDataSearcher(batches);

    SearchResult result = await()
        .until(() -> getQdpSearchResult(request, dataSearcher), r -> r.getResults().size() >= 10);

    assertEquals(10, result.getResults().size());
  }

  @ParameterizedTest(name = "searchFromStreamWithNoBufferingNoParallelism ({arguments})")
  @MethodSource("testSearchers")
  void searchFromStreamWithNoBufferingNoParallelism(SearchRequest.SearchStrategy strategy) {
    var storageRequest = testStorageRequest();
    var filter = getFilterFunction();
    var transformer = getTransformFunction();

    var request = new SearchRequest.Builder()
        .storageName("testStorage")
        .indexName("testIndex")
        .hardLimit(10)
        .pageSize(10)
        .strategy(strategy)
        .request(storageRequest)
        .resultFilter(filter)
        .resultTransformer(transformer)
        .bufferSize(1)
        .build();

    var batches = getBatches(3, 10);
    var dataSearcher = getQdpDataSearcher(batches);
    SearchResult result = await()
        .until(() -> getQdpSearchResult(request, dataSearcher), r -> r.getResults().size() >= 10);
    assertEquals(10, result.getResults().size());
  }

  @ParameterizedTest(name = "emptySearchFromStream ({arguments})")
  @MethodSource("testSearchers")
  void emptySearchFromStream(SearchRequest.SearchStrategy strategy) {
    var storageRequest = testStorageRequest();
    var filter = getFilterFunction();
    var transformer = getTransformFunction();

    var request = new SearchRequest.Builder()
        .storageName("testStorage")
        .indexName("testIndex")
        .hardLimit(10)
        .pageSize(10)
        .strategy(strategy)
        .request(storageRequest)
        .resultFilter(filter)
        .resultTransformer(transformer)
        .bufferSize(8)
        .parallelism(3)
        .build();


    var batches = getBatches(0, 0);
    var dataSearcher = getQdpDataSearcher(batches);
    SearchResult result = getQdpSearchResult(request, dataSearcher);
    assertEquals(0, result.getResults().size());
  }

  @ParameterizedTest(name = "smallSearchFromStream ({arguments})")
  @MethodSource("testSearchers")
  void smallSearchFromStream(SearchRequest.SearchStrategy strategy) {
    var storageRequest = testStorageRequest();
    var filter = getFilterFunction();
    var transformer = getTransformFunction();

    var request = new SearchRequest.Builder()
        .storageName("testStorage")
        .indexName("testIndex")
        .hardLimit(10)
        .pageSize(10)
        .strategy(strategy)
        .request(storageRequest)
        .resultFilter(filter)
        .resultTransformer(transformer)
        .bufferSize(8)
        .parallelism(3)
        .build();


    var batches = getBatches(3, 2);
    var dataSearcher = getQdpDataSearcher(batches);
    SearchResult result = getQdpSearchResult(request, dataSearcher);
    assertEquals(2, result.getResults().size());
  }

  @ParameterizedTest(name = "searchNextFromStream ({arguments})")
  @MethodSource("testSearchers")
  void searchNextFromStream(SearchRequest.SearchStrategy strategy) {
    var storageRequest = testStorageRequest();
    var filter = getFilterFunction();
    var transformer = getTransformFunction();

    var request = new SearchRequest.Builder()
        .storageName("testStorage")
        .indexName("testIndex")
        .hardLimit(10)
        .pageSize(10)
        .strategy(strategy)
        .request(storageRequest)
        .resultFilter(filter)
        .resultTransformer(transformer)
        .bufferSize(4)
        .parallelism(2)
        .build();

    var batches = getBatches(13, 33);
    var dataSearcher = getQdpDataSearcher(batches);

    ActorRef<MoleculeSearchActor.Command> testBehaviour = getTestBehaviorActorRef(request, dataSearcher);
    TestProbe<StatusReply<SearchResult>> probe = testKit.createTestProbe();

    if (strategy == SearchRequest.SearchStrategy.PAGE_FROM_STREAM) {
      await().until(() -> getStatFromTestBehavior(testBehaviour, probe), res -> res.getMatchedByFilterCount() >= 10);
    }
    SearchResult result = getQdpSearchResultFromTestBehavior(testBehaviour, probe);
    assertEquals(10, result.getResults().size());
    assertTrue(getStatFromTestBehavior(testBehaviour, probe).getMatchedByFilterCount() >= 10);

    if (strategy == SearchRequest.SearchStrategy.PAGE_FROM_STREAM) {
      await().until(() -> getStatFromTestBehavior(testBehaviour, probe), res -> res.getMatchedByFilterCount() >= 20);
    }
    result = getQdpSearchResultFromTestBehavior(testBehaviour, probe);
    assertEquals(10, result.getResults().size());
    assertTrue(getStatFromTestBehavior(testBehaviour, probe).getMatchedByFilterCount() >= 20);

    if (strategy == SearchRequest.SearchStrategy.PAGE_FROM_STREAM) {
      await().until(() -> getStatFromTestBehavior(testBehaviour, probe), res -> res.getMatchedByFilterCount() >= 30);
    }
    result = getQdpSearchResultFromTestBehavior(testBehaviour, probe);
    assertEquals(10, result.getResults().size());
    assertTrue(getStatFromTestBehavior(testBehaviour, probe).getMatchedByFilterCount() >= 30);

    if (strategy == SearchRequest.SearchStrategy.PAGE_FROM_STREAM) {
      await().until(() -> getStatFromTestBehavior(testBehaviour, probe), res -> res.getMatchedByFilterCount() >= 33);
    }
    result = getQdpSearchResultFromTestBehavior(testBehaviour, probe);
    assertEquals(3, result.getResults().size());
    assertTrue(getStatFromTestBehavior(testBehaviour, probe).getMatchedByFilterCount() >= 33);
  }

  @ParameterizedTest(name = "searchEmptyNextFromStream ({arguments})")
  @MethodSource("testSearchers")
  void searchEmptyNextFromStream(SearchRequest.SearchStrategy strategy) {
    var storageRequest = testStorageRequest();
    var filter = getFilterFunction();
    var transformer = getTransformFunction();

    var request = new SearchRequest.Builder()
        .storageName("testStorage")
        .indexName("testIndex")
        .hardLimit(10)
        .pageSize(10)
        .strategy(strategy)
        .request(storageRequest)
        .resultFilter(filter)
        .resultTransformer(transformer)
        .bufferSize(1)
        .parallelism(2)
        .build();


    var batches = getBatches(3, 10);
    var dataSearcher = getQdpDataSearcher(batches);
    ActorRef<MoleculeSearchActor.Command> testBehaviour = getTestBehaviorActorRef(request, dataSearcher);
    TestProbe<StatusReply<SearchResult>> probe = testKit.createTestProbe();

    SearchResult result = await()
        .until(() -> getQdpSearchResultFromTestBehavior(testBehaviour, probe), r -> r.getMatchedByFilterCount() >= 10);
    assertEquals(10, result.getResults().size());

    result = getQdpSearchResultFromTestBehavior(testBehaviour, probe);
    assertEquals(0, result.getResults().size());
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
          return " {num=" + number + '}';
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

  private DataSearcher getQdpDataSearcher(List<List<SearchRequest.StorageResultItem>> batches) {
    return new DataSearcher() {
      final Iterator<List<SearchRequest.StorageResultItem>> iterator = batches.iterator();

      @Override
      public List<? extends SearchRequest.StorageResultItem> next() {
        return iterator.hasNext() ? iterator.next() : List.of();
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

  private SearchResult getQdpSearchResult(SearchRequest request, DataSearcher dataSearcher) {
    ActorRef<MoleculeSearchActor.Command> pinger = getTestBehaviorActorRef(request, dataSearcher);
    TestProbe<StatusReply<SearchResult>> probe = testKit.createTestProbe();
    return getQdpSearchResultFromTestBehavior(pinger, probe);
  }

  private SearchResult getQdpSearchResultFromTestBehavior(ActorRef<MoleculeSearchActor.Command> testBehaviour,
                                                          TestProbe<StatusReply<SearchResult>> probe) {
    testBehaviour.tell(new GetNextResult(probe.ref()));
    return probe.receiveMessage(Duration.ofSeconds(10)).getValue();
  }

  private SearchResult getStatFromTestBehavior(ActorRef<MoleculeSearchActor.Command> testBehaviour,
                                               TestProbe<StatusReply<SearchResult>> probe) {
    testBehaviour.tell(new GetStat(probe.ref()));
    return probe.receiveMessage(Duration.ofSeconds(10)).getValue();
  }

  private ActorRef<MoleculeSearchActor.Command> getTestBehaviorActorRef(SearchRequest request,
                                                                        DataSearcher dataSearcher) {
    Behavior<MoleculeSearchActor.Command> commandBehaviorActor = TestBehaviour.create(dataSearcher, request);
    return testKit.spawn(commandBehaviorActor, UUID.randomUUID().toString());
  }

  static class GetNextResult extends MoleculeSearchActor.Search {
    final ActorRef<StatusReply<SearchResult>> replyTo;

    GetNextResult(ActorRef<StatusReply<SearchResult>> replyTo) {
      super(replyTo, null);
      this.replyTo = replyTo;
    }
  }

  static class GetStat extends MoleculeSearchActor.Search {
    final ActorRef<StatusReply<SearchResult>> replyTo;

    GetStat(ActorRef<StatusReply<SearchResult>> replyTo) {
      super(replyTo, null);
      this.replyTo = replyTo;
    }
  }

  static class TestBehaviour extends AbstractBehavior<MoleculeSearchActor.Command> {

    private final Searcher searcher;
    private final SearchRequest searchRequest;

    public TestBehaviour(ActorContext<MoleculeSearchActor.Command> context, DataSearcher dataSearcher,
                         SearchRequest searchRequest) {
      super(context);
      this.searchRequest = searchRequest;

      if (searchRequest.getStrategy() == SearchRequest.SearchStrategy.PAGE_FROM_STREAM) {
        this.searcher = new SearchFlow(context, dataSearcher, searchRequest, UUID.randomUUID().toString());
      } else if (searchRequest.getStrategy() == SearchRequest.SearchStrategy.PAGE_BY_PAGE) {
        this.searcher = new SearchByPage(searchRequest, dataSearcher, UUID.randomUUID().toString());
      } else {
        throw new RuntimeException("Unexpected search strategy: " + searchRequest.getStrategy());
      }
    }

    public static Behavior<MoleculeSearchActor.Command> create(DataSearcher dataSearcher,
                                                               SearchRequest searchRequest) {
      return Behaviors.setup(ctx -> new TestBehaviour(ctx, dataSearcher, searchRequest));
    }

    @Override
    public Receive<MoleculeSearchActor.Command> createReceive() {
      return newReceiveBuilder()
          .onMessage(GetNextResult.class,
              (msgSearch) -> {
                var result = searcher.searchNext(searchRequest.getPageSize());
                List<SearchResultItem> list = new ArrayList<>(result.getResults());

                var stat = searcher.searchStat();
                assertEquals(result.getSearchId(), stat.getSearchId());
                assertEquals(0, stat.getErrorCount());
                assertTrue(stat.getResultCount() >= result.getResultCount());
                assertTrue(stat.getFoundByStorageCount() >= result.getFoundByStorageCount());
                assertTrue(stat.getMatchedByFilterCount() >= result.getMatchedByFilterCount());
                assertNull(stat.getResults());

                while (list.size() < searchRequest.getPageSize() && !result.isSearchFinished()) {
                  result = searcher.searchNext(searchRequest.getPageSize() - list.size());
                  list.addAll(result.getResults());
                }

                msgSearch.replyTo.tell(StatusReply.success(SearchResult.builder()
                    .searchFinished(result.isSearchFinished())
                    .countFinished(result.isCountFinished())
                    .searchId(result.getSearchId())
                    .foundByStorageCount(result.getFoundByStorageCount())
                    .matchedByFilterCount(result.getMatchedByFilterCount())
                    .resultCount(result.getResultCount())
                    .results(list)
                    .build()));
                return Behaviors.same();
              })
          .onMessage(GetStat.class,
              (msgSearch) -> {
                var stat = searcher.searchStat();

                msgSearch.replyTo.tell(StatusReply.success(SearchResult.builder()
                    .searchFinished(stat.isSearchFinished())
                    .countFinished(stat.isCountFinished())
                    .searchId(stat.getSearchId())
                    .foundByStorageCount(stat.getFoundByStorageCount())
                    .matchedByFilterCount(stat.getMatchedByFilterCount())
                    .resultCount(stat.getResultCount())
                    .build()));
                return Behaviors.same();
              })
          .build();
    }
  }

  private SearchRequest.Request testStorageRequest() {
    return new SearchRequest.Request() { };
  }
}