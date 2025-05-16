package com.quantori.cqp.core.source;

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
import com.quantori.cqp.api.SearchIterator;
import com.quantori.cqp.api.model.StorageRequest;
import com.quantori.cqp.core.model.SearchRequest;
import com.quantori.cqp.core.model.SearchResult;
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
  public static final Predicate<TestStorageItem> RESULT_FILTER = item -> true;
  public static final Function<TestStorageItem, TestSearchItem> RESULT_TRANSFORMER =
      item -> new TestSearchItem(item.getId());

  @AfterAll
  static void teardown() {
    testKit.shutdownTestKit();
  }

  private static Stream<Arguments> searchFromStreamArguments() {
    return Stream.of(Arguments.of(3, 10, 10), Arguments.of(0, 0, 0), Arguments.of(3, 2, 2));
  }

  private SearchRequest<TestSearchItem, TestStorageItem> getSearchRequest(int parallelism) {
    return SearchRequest.<TestSearchItem, TestStorageItem>builder()
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
        .parallelism(parallelism)
        .resultFilter(i -> true)
        .resultTransformer(item -> new TestSearchItem(item.getId()))
        .build();
  }

  @ParameterizedTest
  @MethodSource("searchFromStreamArguments")
  void searchFromStream(int batch, int total, int expected) {
    var request = getSearchRequest(3);
    var batches = getBatches(batch, total);
    var searchIterator = getSearchIterator(batches);

    SearchResult<TestSearchItem> result = getSearchResult(request, searchIterator);

    assertEquals(expected, result.getResults().size());
  }

  @Test
  void searchFromStreamWithNoBufferingNoParallelism() {
    var request = getSearchRequest(1);

    var batches = getBatches(3, 10);
    var searchIterator = getSearchIterator(batches);
    SearchResult<TestSearchItem> result = getSearchResult(request, searchIterator);
    assertEquals(10, result.getResults().size());
  }

  @Test
  void searchNextFromStream() {
    var request = getSearchRequest(2);

    var batches = getBatches(13, 33);
    var searchIterator = getSearchIterator(batches);

    ActorRef<SearchActor.Command> testBehaviour = getTestBehaviorActorRef(request, searchIterator);
    var probe = testKit.<StatusReply<SearchResult<TestSearchItem>>>createTestProbe();
    await()
        .until(
            () -> getStatFromTestBehavior(testBehaviour, probe),
            res -> res.getMatchedByFilterCount() >= 10);
    SearchResult<TestSearchItem> result = getSearchResultFromTestBehavior(testBehaviour, probe);
    assertEquals(10, result.getResults().size());
    assertTrue(getStatFromTestBehavior(testBehaviour, probe).getMatchedByFilterCount() >= 10);
    await()
        .until(
            () -> getStatFromTestBehavior(testBehaviour, probe),
            res -> res.getMatchedByFilterCount() >= 20);
    result = getSearchResultFromTestBehavior(testBehaviour, probe);
    assertEquals(10, result.getResults().size());
    assertTrue(getStatFromTestBehavior(testBehaviour, probe).getMatchedByFilterCount() >= 20);
    await()
        .until(
            () -> getStatFromTestBehavior(testBehaviour, probe),
            res -> res.getMatchedByFilterCount() >= 30);
    result = getSearchResultFromTestBehavior(testBehaviour, probe);
    assertEquals(10, result.getResults().size());
    assertTrue(getStatFromTestBehavior(testBehaviour, probe).getMatchedByFilterCount() >= 30);
    await()
        .until(
            () -> getStatFromTestBehavior(testBehaviour, probe),
            res -> res.getMatchedByFilterCount() >= 33);
    result = getSearchResultFromTestBehavior(testBehaviour, probe);
    assertEquals(3, result.getResults().size());
    assertTrue(getStatFromTestBehavior(testBehaviour, probe).getMatchedByFilterCount() >= 33);
  }

  @Test
  void searchEmptyNextFromStream() {
    var request = getSearchRequest(2);
    var batches = getBatches(3, 10);
    var searchIterator = getSearchIterator(batches);
    ActorRef<SearchActor.Command> testBehaviour = getTestBehaviorActorRef(request, searchIterator);
    var probe = testKit.<StatusReply<SearchResult<TestSearchItem>>>createTestProbe();

    SearchResult<TestSearchItem> result =
        await()
            .until(
                () -> getSearchResultFromTestBehavior(testBehaviour, probe),
                r -> {
                  System.out.println(r.getMatchedByFilterCount());
                  return r.getMatchedByFilterCount() >= 10;
                });
    assertEquals(10, result.getResults().size());

    result = getSearchResultFromTestBehavior(testBehaviour, probe);
    assertEquals(0, result.getResults().size());
  }

  private List<List<TestStorageItem>> getBatches(int batch, int total) {
    List<List<TestStorageItem>> batches = new ArrayList<>();
    List<TestStorageItem> batchArray = new ArrayList<>();
    int totalCount = 0;
    while (total > totalCount) {
      var next =
          new TestStorageItem() {
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

  private SearchIterator<TestStorageItem> getSearchIterator(
      List<List<TestStorageItem>> batches) {
    return new SearchIterator<>() {
      final Iterator<List<TestStorageItem>> iterator = batches.iterator();

      @Override
      public List<TestStorageItem> next() {
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
      public void close() {}
    };
  }

  private SearchResult<TestSearchItem> getSearchResult(
      SearchRequest<TestSearchItem, TestStorageItem> request,
      SearchIterator<TestStorageItem> searchIterator) {
    ActorRef<SearchActor.Command> pinger = getTestBehaviorActorRef(request, searchIterator);
    var probe = testKit.<StatusReply<SearchResult<TestSearchItem>>>createTestProbe();
    return getSearchResultFromTestBehavior(pinger, probe);
  }

  private SearchResult<TestSearchItem> getSearchResultFromTestBehavior(
      ActorRef<SearchActor.Command> testBehaviour,
      TestProbe<StatusReply<SearchResult<TestSearchItem>>> probe) {
    testBehaviour.tell(new GetNextResult(probe.ref(), 10));
    return probe.receiveMessage(Duration.ofSeconds(10)).getValue();
  }

  private SearchResult<TestSearchItem> getStatFromTestBehavior(
      ActorRef<SearchActor.Command> testBehaviour,
      TestProbe<StatusReply<SearchResult<TestSearchItem>>> probe) {
    testBehaviour.tell(new GetStat(probe.ref()));
    return probe.receiveMessage(Duration.ofSeconds(10)).getValue();
  }

  private ActorRef<SearchActor.Command> getTestBehaviorActorRef(
      SearchRequest<TestSearchItem, TestStorageItem> request,
      SearchIterator<TestStorageItem> searchIterator) {
    Behavior<SearchActor.Command> commandBehaviorActor =
        TestBehaviour.create(searchIterator, request);
    return testKit.spawn(commandBehaviorActor, UUID.randomUUID().toString());
  }

  static class GetNextResult extends SearchActor.Search<TestSearchItem, TestStorageItem> {
    final ActorRef<StatusReply<SearchResult<TestSearchItem>>> replyTo;
    final int count;

    GetNextResult(ActorRef<StatusReply<SearchResult<TestSearchItem>>> replyTo, int count) {
      super(replyTo, null);
      this.replyTo = replyTo;
      this.count = count;
    }
  }

  static class GetStat extends SearchActor.Search<TestSearchItem, TestStorageItem> {
    final ActorRef<StatusReply<SearchResult<TestSearchItem>>> replyTo;

    GetStat(ActorRef<StatusReply<SearchResult<TestSearchItem>>> replyTo) {
      super(replyTo, null);
      this.replyTo = replyTo;
    }
  }

  static class TestBehaviour extends AbstractBehavior<SearchActor.Command> {

    private final Searcher<TestSearchItem, TestStorageItem> searcher;

    public TestBehaviour(
        ActorContext<SearchActor.Command> context,
        SearchIterator<TestStorageItem> searchIterator,
        SearchRequest<TestSearchItem, TestStorageItem> testSearchRequest) {
      super(context);
      this.searcher =
          new Searcher<>(
              context,
              Map.of(TEST_STORAGE, List.of(searchIterator)),
              testSearchRequest,
              UUID.randomUUID().toString());
    }

    public static Behavior<SearchActor.Command> create(
        SearchIterator<TestStorageItem> searchIterator,
        SearchRequest<TestSearchItem, TestStorageItem> testSearchRequest) {
      return Behaviors.setup(ctx -> new TestBehaviour(ctx, searchIterator, testSearchRequest));
    }

    @Override
    public Receive<SearchActor.Command> createReceive() {
      return newReceiveBuilder()
          .onMessage(
              GetNextResult.class,
              (msgSearch) -> {
                var result = searcher.getSearchResult(msgSearch.count).toCompletableFuture().join();
                List<TestSearchItem> list = new ArrayList<>(result.getResults());

                msgSearch.replyTo.tell(
                    StatusReply.success(
                        SearchResult.<TestSearchItem>builder()
                            .searchFinished(result.isSearchFinished())
                            .searchId(result.getSearchId())
                            .foundCount(result.getFoundCount())
                            .matchedByFilterCount(result.getMatchedByFilterCount())
                            .resultCount(result.getResultCount())
                            .results(list)
                            .build()));
                return Behaviors.same();
              })
          .onMessage(
              GetStat.class,
              (msgSearch) -> {
                var stat = searcher.getSearchResult(0).toCompletableFuture().join();

                msgSearch.replyTo.tell(
                    StatusReply.success(
                        SearchResult.<TestSearchItem>builder()
                            .searchFinished(stat.isSearchFinished())
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
}
