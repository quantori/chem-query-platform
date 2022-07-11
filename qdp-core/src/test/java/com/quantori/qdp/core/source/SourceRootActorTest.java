package com.quantori.qdp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.*;
import com.quantori.qdp.core.configuration.ClusterConfigurationProperties;
import com.quantori.qdp.core.configuration.LocalClusterProvider;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;

public class SourceRootActorTest {

    @Test
    public void testRegisterActor() {
        ActorSystem<SourceRootActor.Command> actorSystem = new LocalClusterProvider().actorTypedSystem(ClusterConfigurationProperties.builder().maxSearchActors(10).build());
        AtomicBoolean called = new AtomicBoolean(false);
        SourceRootActor.StartedActor<String> result = AskPattern.ask(actorSystem, (ActorRef<SourceRootActor.StartedActor<String>> replyTo) ->
                        new SourceRootActor.StartActor<>(Behaviors.setup(ctx -> new TestActor(ctx, called)), replyTo),
                Duration.ofMinutes(1), actorSystem.scheduler()).toCompletableFuture().join();
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
            return newReceiveBuilder().onMessage(String.class, str -> {
                this.called.set(true);
                return Behaviors.same();
            }).build();
        }
    }
}
