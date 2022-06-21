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
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterConfiguration {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterConfiguration.class);
  public static final String QDP_AKKA_SYSTEM = "qdp-akka-system";

  public ActorSystem<MoleculeSourceRootActor.Command> actorTypedSystem() {
    Map<String, String> environment = System.getenv();
    Set<String> activeProfiles = Arrays.stream(environment.get("profile")
        .split(",")).collect(Collectors.toSet());
    int maxAmountOfSearchActors =
        Integer.parseInt(environment.getOrDefault("app.search.actors.max", "100"));

    if (activeProfiles.contains("ecs")) {
      String metadataUri = environment.get("ECS_CONTAINER_METADATA_URI");
      Config config = ConfigFactory.parseMap(ECSConfigurationProvider.getConfiguration(metadataUri))
          .withFallback(ConfigFactory.load("akka-cluster-ecs"));
      var akkaSystem =
          ActorSystem.create(MoleculeSourceRootActor.create(maxAmountOfSearchActors),
              QDP_AKKA_SYSTEM,
              config);

      AkkaManagement.get(akkaSystem).start();
      ClusterBootstrap.get(akkaSystem).start();
      return akkaSystem;
    } else if (activeProfiles.contains("test")) {
      String hostName = environment.getOrDefault("app.cluster.hostname", "localhost");
      int port = Integer.parseInt(environment.getOrDefault("app.cluster.port", "2551"));
      String nodes = environment.getOrDefault("app.cluster.nodes", "'localhost:2551'");

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

  public akka.actor.ActorSystem actorSystem() {
    akka.actor.ActorSystem system = actorTypedSystem().classicSystem();
    system.registerOnTermination(callback());
    return system;
  }

  private Runnable callback() {
    return () -> LOGGER.info("shutting down {} actor system", QDP_AKKA_SYSTEM);
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
