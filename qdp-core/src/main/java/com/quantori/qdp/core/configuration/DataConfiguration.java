package com.quantori.qdp.core.configuration;

import akka.actor.typed.ActorSystem;
import akka.management.cluster.bootstrap.ClusterBootstrap;
import akka.management.javadsl.AkkaManagement;
import com.quantori.qdp.core.source.MoleculeSourceRootActor;
import com.quantori.qdp.core.utilities.ECSConfigurationProvider;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.lang.NonNull;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@ComponentScan
@EnableAutoConfiguration
@EnableTransactionManagement
@EnableJpaRepositories
public class DataConfiguration {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataConfiguration.class);
  public static final String QDP_AKKA_SYSTEM = "qdp-akka-system";

  @Bean
  @Profile("ecs")
  public ActorSystem<MoleculeSourceRootActor.Command> actorTypedSystem(
      ApplicationContext context,
      @Value("${app.search.actors.max:100}") int maxAmountOfSearchActors,
      @Value("${app.cluster.hostname:localhost}") String hostName,
      @Value("${app.cluster.port:2551}") int port,
      @Value("${app.cluster.nodes:'localhost:2551'}") String nodes
  ) {
    if (Set.of(context.getEnvironment().getActiveProfiles()).contains("ecs")) {
      String metadataUri = context.getEnvironment().getProperty("ECS_CONTAINER_METADATA_URI");
      Config config = ConfigFactory.parseMap(ECSConfigurationProvider.getConfiguration(metadataUri))
          .withFallback(ConfigFactory.load("akka-cluster-ecs"));
      var akkaSystem =
          ActorSystem.create(MoleculeSourceRootActor.create(maxAmountOfSearchActors),
              QDP_AKKA_SYSTEM,
              config);

      AkkaManagement.get(akkaSystem).start();
      ClusterBootstrap.get(akkaSystem).start();
      return akkaSystem;
    } else if (!Set.of(context.getEnvironment().getActiveProfiles()).contains("test")) {
      LOGGER.info("app.cluster.hostname = {}", hostName);
      LOGGER.info("app.cluster.port = {}", port);
      LOGGER.info("app.cluster.nodes = {}", nodes);

      Map<String, Object> overrides = new HashMap<>();
      overrides.put("akka.remote.artery.canonical.hostname", hostName);
      overrides.put("akka.remote.artery.canonical.port", port);
      overrides.put("akka.cluster.seed-nodes", getSeedNodes(nodes));

      Config config = ConfigFactory.parseMap(overrides)
          .withFallback(ConfigFactory.load("akka-cluster"));

      return ActorSystem.create(MoleculeSourceRootActor.create(maxAmountOfSearchActors),
          QDP_AKKA_SYSTEM, config);
    } else {
      return ActorSystem.create(MoleculeSourceRootActor.create(maxAmountOfSearchActors),
          QDP_AKKA_SYSTEM);
    }
  }

  @Bean(destroyMethod = "")
  public akka.actor.ActorSystem actorSystem(ActorSystem<MoleculeSourceRootActor.Command> system) {
    return system.classicSystem();
  }

  @Bean
  public ActorSystemShutdownHelper actorSystemShutdownHelper(akka.actor.ActorSystem actorSystem) {
    return new ActorSystemShutdownHelper(actorSystem);
  }

  public static class ActorSystemShutdownHelper implements SmartLifecycle {
    private final akka.actor.ActorSystem actorSystem;

    public ActorSystemShutdownHelper(akka.actor.ActorSystem actorSystem) {
      this.actorSystem = actorSystem;
    }

    @Override
    public void start() {
      // No-op
    }

    @Override
    public void stop() {
      throw new UnsupportedOperationException("The stop() method must never be called");
    }

    @Override
    public void stop(@NonNull Runnable callback) {
      if (actorSystem.whenTerminated().isCompleted()) {
        callback.run();
      } else {
        actorSystem.registerOnTermination(callback);
        actorSystem.terminate();
      }
    }

    @Override
    public boolean isRunning() {
      return !actorSystem.whenTerminated().isCompleted();
    }

    @Override
    public int getPhase() {
      return -10;
    }
  }

  private List<String> getSeedNodes(String nodes) {
    return Arrays.stream(nodes.split(","))
        .map(String::trim)
        .filter(StringUtils::isNotBlank)
        .map(string -> string.startsWith("akka")
            ? string
            : String.format("akka://%s@%s", QDP_AKKA_SYSTEM, string))
        .toList();
  }
}
