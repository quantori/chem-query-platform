package com.quantori.cqp.core.source;

import static org.awaitility.Awaitility.await;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.quantori.cqp.core.configuration.ClusterConfigurationProperties;
import com.quantori.cqp.core.configuration.LocalClusterProvider;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import com.quantori.cqp.core.source.SourceRootActor;
import org.junit.jupiter.api.Test;

class SourceRootActorTest {

  @Test
  void testRegisterActor() {
    ActorSystem<SourceRootActor.Command> actorSystem =
        new LocalClusterProvider()
            .actorTypedSystem(ClusterConfigurationProperties.builder().maxSearchActors(10).build());
    AtomicBoolean called = new AtomicBoolean(false);
    SourceRootActor.StartedActor<String> result =
        AskPattern.ask(
                actorSystem,
                (ActorRef<SourceRootActor.StartedActor<String>> replyTo) ->
                    new SourceRootActor.StartActor<>(
                        Behaviors.setup(ctx -> new TestActor(ctx, called)), replyTo),
                Duration.ofMinutes(1),
                actorSystem.scheduler())
            .toCompletableFuture()
            .join();
    result.actorRef.tell("Hello");
    await().atMost(Duration.ofSeconds(5)).until(called::get);
  }

  private static class TestActor extends AbstractBehavior<String> {

    private final AtomicBoolean called;

    public TestActor(ActorContext<String> context, AtomicBoolean called) {
      super(context);
      this.called = called;
    }

    @Override
    public Receive<String> createReceive() {
      return newReceiveBuilder()
          .onMessage(
              String.class,
              str -> {
                this.called.set(true);
                return Behaviors.same();
              })
          .build();
    }
  }
}
