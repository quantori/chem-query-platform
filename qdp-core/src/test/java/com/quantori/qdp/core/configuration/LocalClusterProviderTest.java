package com.quantori.qdp.core.configuration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

import akka.actor.typed.ActorSystem;
import com.quantori.qdp.core.source.MoleculeSourceRootActor;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class LocalClusterProviderTest {

  @Test
  void localClusterStartsWithNoError() throws InterruptedException {
    LocalClusterProvider localClusterProvider = new LocalClusterProvider();

    ClusterConfigurationProperties properties = ClusterConfigurationProperties.builder()
        .maxSearchActors(100)
        .build();

    ActorSystem<MoleculeSourceRootActor.Command> system = null;
    try {
      system = localClusterProvider.actorTypedSystem(properties);

      Thread.sleep(2001);

      long uptime = system.uptime();

      assertThat(system, is(notNullValue()));
      assertThat(uptime, is(greaterThan(2L)));
    } finally {
      Objects.requireNonNull(system).terminate();
    }
  }
}