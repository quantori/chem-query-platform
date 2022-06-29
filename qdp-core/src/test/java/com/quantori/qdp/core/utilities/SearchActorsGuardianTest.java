package com.quantori.qdp.core.utilities;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import com.quantori.qdp.core.source.MoleculeSearchActor;
import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import com.quantori.qdp.core.source.model.molecule.search.SearchResult;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

class SearchActorsGuardianTest {
  Logger log = LoggerFactory.getLogger(SearchActorsGuardianTest.class);
  static final ActorTestKit testKit = ActorTestKit.create();

  @AfterAll
  static void teardown() {
    testKit.shutdownTestKit();
  }

  @Test
  void testGuardian() throws Exception {
    int expectedAmount = 10;
    int spawnedAmount = 15;

    testKit.spawn(SearchActorsGuardian.create(expectedAmount), "name");
    CountDownLatch cdl = new CountDownLatch(spawnedAmount - expectedAmount);
    List<Boolean> marks = Collections.synchronizedList(new ArrayList<>(spawnedAmount));

    spawnActors(spawnedAmount, cdl, marks);

    boolean resultSuccess = cdl.await(2, TimeUnit.SECONDS);
    log.debug("cdl count: " + cdl.getCount());
    Assertions.assertTrue(resultSuccess);

    Set<ActorRef<MoleculeSearchActor.Command>> actors = getActorRefsFromReceptionist(expectedAmount);

    Assertions.assertEquals(expectedAmount, actors.size());
    Assertions.assertFalse(marks.get(0));
    Assertions.assertFalse(marks.get(1));
    Assertions.assertFalse(marks.get(2));
    Assertions.assertFalse(marks.get(3));
    Assertions.assertFalse(marks.get(4));
    Assertions.assertTrue(marks.get(5));
    Assertions.assertTrue(marks.get(6));
    Assertions.assertTrue(marks.get(7));
    Assertions.assertTrue(marks.get(8));
    Assertions.assertTrue(marks.get(9));
    Assertions.assertTrue(marks.get(10));
    Assertions.assertTrue(marks.get(11));
    Assertions.assertTrue(marks.get(12));
    Assertions.assertTrue(marks.get(13));
    Assertions.assertTrue(marks.get(14));

  }

  private void spawnActors(int spawnedAmount, CountDownLatch cdl, List<Boolean> marks) throws InterruptedException {
    for (int count = 0; count < spawnedAmount; count++) {
      marks.add(Boolean.TRUE);
      testKit.spawn(SomeSearchActor.create(cdl, marks, count), "name" + count);
      Thread.sleep(10);
    }
  }

  private Set<ActorRef<MoleculeSearchActor.Command>> getActorRefsFromReceptionist(int expectedAmount) throws InterruptedException, java.util.concurrent.ExecutionException {
    Set<ActorRef<MoleculeSearchActor.Command>> actors = new HashSet<>();
    int attemptsNumber = 0;
    while (actors.size() != expectedAmount && attemptsNumber < 10) {
      Thread.sleep(100);

      actors.clear();
      ServiceKey<MoleculeSearchActor.Command> serviceKey = MoleculeSearchActor.searchActorsKey;

      CompletionStage<Receptionist.Listing> cf = AskPattern.ask(
          testKit.system().receptionist(),
          ref -> Receptionist.find(serviceKey, ref),
          Duration.ofMinutes(1),
          testKit.scheduler());

      actors.addAll(cf.toCompletableFuture().get().getServiceInstances(serviceKey));
      attemptsNumber++;
    }
    return actors;
  }

  static class SomeSearchActor extends MoleculeSearchActor {
    final CountDownLatch cdl;
    final List<Boolean> marks;
    final int count;

    public static Behavior<Command> create(CountDownLatch cdl, List<Boolean> marks, int count) {
      return Behaviors.setup(ctx ->
          Behaviors.withTimers(timer -> new SomeSearchActor(ctx, "storageName", timer, cdl, marks, count)));
    }

    public SomeSearchActor(ActorContext<MoleculeSearchActor.Command> context,
                           String storageName,
                           TimerScheduler<MoleculeSearchActor.Command> timer,
                           CountDownLatch cdl,
                           List<Boolean> marks,
                           int count) {
      super(context, storageName, timer);
      this.cdl = cdl;
      this.marks = marks;
      this.count = count;
      getContext().getSystem().receptionist().tell(Receptionist.register(MoleculeSearchActor.searchActorsKey, context.getSelf()));
    }

    @Override
    protected CompletionStage<SearchResult> search(SearchRequest searchRequest) {
      return null;
    }

    @Override
    protected CompletionStage<SearchResult> searchNext(int limit) {
      return null;
    }

    @Override
    protected CompletionStage<SearchResult> searchStatistics() {
      return null;
    }

    @Override
    protected SearchRequest getSearchRequest() {
      return  new SearchRequest.Builder().build();
    }

    @Override
    protected void onTerminate() {
      cdl.countDown();
      marks.set(count, Boolean.FALSE);
    }
  }
}