package com.quantori.cqp.core.configuration;

import akka.actor.typed.ActorSystem;
import com.quantori.cqp.core.source.SourceRootActor;
import org.awaitility.Awaitility;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

class ClusterProviderTest {

    @Test
    void configuredClusterStartsWithNoError() throws Exception {
        ClusterProvider clusterProvider = new ClusterProvider();

        ClusterConfigurationProperties nodeProperties1 =
            ClusterConfigurationProperties.builder()
                .clusterHostName("localhost")
                .clusterPort(8080)
                .maxSearchActors(100)
                .seedNodes(List.of("localhost:8080", "localhost:8081", "localhost:8082"))
                .build();

        ClusterConfigurationProperties nodeProperties2 =
            ClusterConfigurationProperties.builder()
                .clusterHostName("localhost")
                .clusterPort(8084)
                .maxSearchActors(100)
                .seedNodes(List.of("localhost:8083"))
                .build();

        ActorSystem<SourceRootActor.Command> system1 = null;
        ActorSystem<SourceRootActor.Command> system2 = null;
        try {
            system1 = clusterProvider.actorTypedSystem(nodeProperties1);
            system2 = clusterProvider.actorTypedSystem(nodeProperties2);

            assertThat(system1, is(notNullValue()));
            assertThat(system2, is(notNullValue()));

            Callable<Long> cpSystem1 = system1::uptime;
            Callable<Long> cpSystem2 = system2::uptime;
            Awaitility.await("cluster start").until(cpSystem1, Matchers.<Long>greaterThan(2L));
            Awaitility.await("cluster start").until(cpSystem2, Matchers.<Long>greaterThan(2L));
        } finally {
            Objects.requireNonNull(system1).terminate();
            Objects.requireNonNull(system2).terminate();
            Await.result(system1.whenTerminated(), Duration.apply(5, TimeUnit.SECONDS));
            Await.result(system2.whenTerminated(), Duration.apply(5, TimeUnit.SECONDS));
        }
    }
}
