package com.quantori.cqp.core.configuration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

import akka.actor.typed.ActorSystem;
import com.quantori.cqp.core.configuration.ClusterConfigurationProperties;
import com.quantori.cqp.core.configuration.LocalClusterProvider;
import com.quantori.cqp.core.source.SourceRootActor;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

class LocalClusterProviderTest {

  @Test
  void localClusterStartsWithNoError() throws Exception {
    LocalClusterProvider localClusterProvider = new LocalClusterProvider();

    ClusterConfigurationProperties properties =
        ClusterConfigurationProperties.builder().maxSearchActors(100).build();

    ActorSystem<SourceRootActor.Command> system = null;
    try {
      system = localClusterProvider.actorTypedSystem(properties);

      Thread.sleep(2001);

      long uptime = system.uptime();

      assertThat(system, is(notNullValue()));
      assertThat(uptime, is(greaterThanOrEqualTo(2L)));
    } finally {
      Objects.requireNonNull(system).terminate();
      Await.result(system.whenTerminated(), Duration.apply(5, TimeUnit.SECONDS));
    }
  }
}
