package com.quantori.qdp.core.source;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import com.quantori.qdp.core.source.model.DataSearcher;
import com.quantori.qdp.core.source.model.MultiStorageSearchRequest;
import com.quantori.qdp.core.source.model.ProcessingSettings;
import com.quantori.qdp.core.source.model.RequestStructure;
import com.quantori.qdp.core.source.model.SearchResult;
import com.quantori.qdp.core.source.model.StorageItem;
import com.quantori.qdp.core.source.model.StorageRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SearcherTest {

  static final ActorTestKit testKit = ActorTestKit.create();
  public static final String TEST_STORAGE = "testStorage";
  public static final String TEST_INDEX = "testIndex";

  @AfterAll
  static void teardown() {
    testKit.shutdownTestKit();
  }

  private static Stream<Arguments> searchFromStreamArguments() {
    return Stream.of(
        Arguments.of(3, 10, 10),
        Arguments.of(0, 0, 0),
        Arguments.of(3, 2, 2)
    );
  }

  private SearchRequest getSearchRequest(StorageRequest storageRequest, Predicate<StorageItem> filter,
                                         Function<StorageItem, Molecule> transformer, int bufferSize, int parallelism) {
    return SearchRequest.builder()
        .requestStructure(RequestStructure.<Molecule>builder()
            .storageName(TEST_STORAGE)
            .indexNames(List.of(TEST_INDEX))
            .storageRequest(storageRequest)
            .resultFilter(filter)
            .resultTransformer(transformer)
            .build())
        .processingSettings(ProcessingSettings.builder()
            .bufferSize(bufferSize)
            .parallelism(parallelism)
            .build())
        .build();
  }

  @ParameterizedTest
  @MethodSource("searchFromStreamArguments")
  void searchFromStream(int batch, int total, int expected) {
    var storageRequest = testStorageRequest();
    var filter = getFilterFunction();
    var transformer = getTransformFunction();
    var request = getSearchRequest(storageRequest, filter, transformer, 8, 3);
    var batches = getBatches(batch, total);
    var dataSearcher = getQdpDataSearcher(batches);

    SearchResult<Molecule> result = getQdpSearchResult(request, dataSearcher);

    assertEquals(expected, result.getResults().size());
  }

  @Test
  void searchFromStreamWithNoBufferingNoParallelism() {
    var storageRequest = testStorageRequest();
    var filter = getFilterFunction();
    var transformer = getTransformFunction();

    var request = SearchRequest.builder()
        .requestStructure(RequestStructure.<Molecule>builder()
            .storageName(TEST_STORAGE)
            .indexNames(List.of(TEST_INDEX))
            .storageRequest(storageRequest)
            .resultFilter(filter)
            .resultTransformer(transformer)
            .build())
        .processingSettings(ProcessingSettings.builder()
            .bufferSize(1)
            .build())
        .build();

    var batches = getBatches(3, 10);
    var dataSearcher = getQdpDataSearcher(batches);
    SearchResult<Molecule> result = getQdpSearchResult(request, dataSearcher);
    assertEquals(10, result.getResults().size());
  }

  @Test
  void searchNextFromStream() {
    var storageRequest = testStorageRequest();
    var filter = getFilterFunction();
    var transformer = getTransformFunction();

    var request = getSearchRequest(storageRequest, filter, transformer, 10, 2);

    var batches = getBatches(13, 33);
    var dataSearcher = getQdpDataSearcher(batches);

    ActorRef<SearchActor.Command> testBehaviour = getTestBehaviorActorRef(request, dataSearcher);
    var probe = testKit.<StatusReply<SearchResult<Molecule>>>createTestProbe();
    await().until(() -> getStatFromTestBehavior(testBehaviour, probe), res -> res.getMatchedByFilterCount() >= 10);
    SearchResult<Molecule> result = getQdpSearchResultFromTestBehavior(testBehaviour, probe);
    assertEquals(10, result.getResults().size());
    assertTrue(getStatFromTestBehavior(testBehaviour, probe).getMatchedByFilterCount() >= 10);
    await().until(() -> getStatFromTestBehavior(testBehaviour, probe), res -> res.getMatchedByFilterCount() >= 20);
    result = getQdpSearchResultFromTestBehavior(testBehaviour, probe);
    assertEquals(10, result.getResults().size());
    assertTrue(getStatFromTestBehavior(testBehaviour, probe).getMatchedByFilterCount() >= 20);
    await().until(() -> getStatFromTestBehavior(testBehaviour, probe), res -> res.getMatchedByFilterCount() >= 30);
    result = getQdpSearchResultFromTestBehavior(testBehaviour, probe);
    assertEquals(10, result.getResults().size());
    assertTrue(getStatFromTestBehavior(testBehaviour, probe).getMatchedByFilterCount() >= 30);
    await().until(() -> getStatFromTestBehavior(testBehaviour, probe), res -> res.getMatchedByFilterCount() >= 33);
    result = getQdpSearchResultFromTestBehavior(testBehaviour, probe);
    assertEquals(3, result.getResults().size());
    assertTrue(getStatFromTestBehavior(testBehaviour, probe).getMatchedByFilterCount() >= 33);
  }

  @Test
  void searchEmptyNextFromStream() {
    var storageRequest = testStorageRequest();
    var filter = getFilterFunction();
    var transformer = getTransformFunction();

    var request = getSearchRequest(storageRequest, filter, transformer, 1, 2);
    var batches = getBatches(3, 10);
    var dataSearcher = getQdpDataSearcher(batches);
    ActorRef<SearchActor.Command> testBehaviour = getTestBehaviorActorRef(request, dataSearcher);
    var probe = testKit.<StatusReply<SearchResult<Molecule>>>createTestProbe();

    SearchResult<Molecule> result = await()
        .until(() -> getQdpSearchResultFromTestBehavior(testBehaviour, probe), r -> {
          System.out.println(r.getMatchedByFilterCount());
          return r.getMatchedByFilterCount() >= 10;
        });
    assertEquals(10, result.getResults().size());

    result = getQdpSearchResultFromTestBehavior(testBehaviour, probe);
    assertEquals(0, result.getResults().size());
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

  private DataSearcher getQdpDataSearcher(List<List<StorageItem>> batches) {
    return new DataSearcher() {
      final Iterator<List<StorageItem>> iterator = batches.iterator();

      @Override
      public List<? extends StorageItem> next() {
        return iterator.hasNext() ? iterator.next() : List.of();
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

  private SearchResult<Molecule> getQdpSearchResult(SearchRequest request, DataSearcher dataSearcher) {
    ActorRef<SearchActor.Command> pinger = getTestBehaviorActorRef(request, dataSearcher);
    var probe = testKit.<StatusReply<SearchResult<Molecule>>>createTestProbe();
    return getQdpSearchResultFromTestBehavior(pinger, probe);
  }

  private SearchResult<Molecule> getQdpSearchResultFromTestBehavior(
      ActorRef<SearchActor.Command> testBehaviour, TestProbe<StatusReply<SearchResult<Molecule>>> probe) {
    testBehaviour.tell(new GetNextResult(probe.ref(), 10));
    return probe.receiveMessage(Duration.ofSeconds(10)).getValue();
  }

  private SearchResult<Molecule> getStatFromTestBehavior(ActorRef<SearchActor.Command> testBehaviour,
                                                         TestProbe<StatusReply<SearchResult<Molecule>>> probe) {
    testBehaviour.tell(new GetStat(probe.ref()));
    return probe.receiveMessage(Duration.ofSeconds(10)).getValue();
  }

  private ActorRef<SearchActor.Command> getTestBehaviorActorRef(SearchRequest request,
                                                                DataSearcher dataSearcher) {
    Behavior<SearchActor.Command> commandBehaviorActor = TestBehaviour.create(dataSearcher, request);
    return testKit.spawn(commandBehaviorActor, UUID.randomUUID().toString());
  }

  static class GetNextResult extends SearchActor.Search<Molecule> {
    final ActorRef<StatusReply<SearchResult<Molecule>>> replyTo;
    final int count;

    GetNextResult(ActorRef<StatusReply<SearchResult<Molecule>>> replyTo, int count) {
      super(replyTo, null);
      this.replyTo = replyTo;
      this.count = count;
    }
  }

  static class GetStat extends SearchActor.Search<Molecule> {
    final ActorRef<StatusReply<SearchResult<Molecule>>> replyTo;

    GetStat(ActorRef<StatusReply<SearchResult<Molecule>>> replyTo) {
      super(replyTo, null);
      this.replyTo = replyTo;
    }
  }

  static class TestBehaviour extends AbstractBehavior<SearchActor.Command> {

    private final Searcher<Molecule> searcher;

    public TestBehaviour(ActorContext<SearchActor.Command> context, DataSearcher dataSearcher,
                         SearchRequest searchRequest) {
      super(context);
      MultiStorageSearchRequest<Molecule> searchRequest1 = MultiStorageSearchRequest.<Molecule>builder()
          .requestStorageMap(Map.of(TEST_STORAGE, searchRequest.getRequestStructure()))
          .processingSettings(searchRequest.getProcessingSettings())
          .build();
      this.searcher = new SearchFlow<>(context, Map.of(TEST_STORAGE, List.of(dataSearcher)), searchRequest1,
          UUID.randomUUID().toString());
    }

    public static Behavior<SearchActor.Command> create(DataSearcher dataSearcher,
                                                       SearchRequest searchRequest) {
      return Behaviors.setup(ctx -> new TestBehaviour(ctx, dataSearcher, searchRequest));
    }

    @Override
    public Receive<SearchActor.Command> createReceive() {
      return newReceiveBuilder()
          .onMessage(GetNextResult.class,
              (msgSearch) -> {
                var result = searcher
                    .searchNext(msgSearch.count).toCompletableFuture().join();
                List<Molecule> list = new ArrayList<>(result.getResults());

                msgSearch.replyTo.tell(StatusReply.success(SearchResult.<Molecule>builder()
                    .searchFinished(result.isSearchFinished())
                    .countFinished(result.isCountFinished())
                    .searchId(result.getSearchId())
                    .foundCount(result.getFoundCount())
                    .matchedByFilterCount(result.getMatchedByFilterCount())
                    .resultCount(result.getResultCount())
                    .results(list)
                    .build()));
                return Behaviors.same();
              })
          .onMessage(GetStat.class,
              (msgSearch) -> {
                var stat = searcher.searchNext(0).toCompletableFuture().join();

                msgSearch.replyTo.tell(StatusReply.success(SearchResult.<Molecule>builder()
                    .searchFinished(stat.isSearchFinished())
                    .countFinished(stat.isCountFinished())
                    .searchId(stat.getSearchId())
                    .foundCount(stat.getFoundCount())
                    .matchedByFilterCount(stat.getMatchedByFilterCount())
                    .resultCount(stat.getResultCount())
                    .build()));
                return Behaviors.same();
              })
          .build();
    }
  }

  private StorageRequest testStorageRequest() {
    return new StorageRequest() {
    };
  }
}