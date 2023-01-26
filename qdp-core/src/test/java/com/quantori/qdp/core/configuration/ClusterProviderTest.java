package com.quantori.qdp.core.configuration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

import akka.actor.typed.ActorSystem;
import com.quantori.qdp.core.source.SourceRootActor;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

class ClusterProviderTest {

  @Test
  void configuredClusterStartsWithNoError() throws Exception {
    ClusterProvider clusterProvider = new ClusterProvider();

    ClusterConfigurationProperties nodeProperties1 = ClusterConfigurationProperties.builder()
        .clusterHostName("localhost")
        .clusterPort(8080)
        .maxSearchActors(100)
        .seedNodes(List.of("localhost:8080", "localhost:8081", "localhost:8082"))
        .build();

    ClusterConfigurationProperties nodeProperties2 = ClusterConfigurationProperties.builder()
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

      Thread.sleep(3000);

      long uptime1 = system1.uptime();
      long uptime2 = system2.uptime();

      assertThat(system1, is(notNullValue()));
      assertThat(uptime1, is(greaterThan(2L)));
      assertThat(system2, is(notNullValue()));
      assertThat(uptime2, is(greaterThan(2L)));
    } finally {
      Objects.requireNonNull(system1).terminate();
      Objects.requireNonNull(system2).terminate();
      Await.result(system1.whenTerminated(), Duration.apply(5, TimeUnit.SECONDS));
      Await.result(system2.whenTerminated(), Duration.apply(5, TimeUnit.SECONDS));
    }
  }
}
