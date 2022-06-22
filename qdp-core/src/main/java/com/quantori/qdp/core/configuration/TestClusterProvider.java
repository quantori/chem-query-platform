package com.quantori.qdp.core.configuration;

import akka.actor.typed.ActorSystem;
import com.quantori.qdp.core.source.MoleculeSourceRootActor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestClusterProvider implements ClusterProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(TestClusterProvider.class);

  @Override
  public ActorSystem<MoleculeSourceRootActor.Command> actorTypedSystem(ClusterConfigurationProperties properties) {
    String hostName = properties.clusterHostName();
    int port = properties.clusterPort();
    List<String> seedNodes = properties.seedNodes();

    LOGGER.info("app.cluster.hostname = {}", hostName);
    LOGGER.info("app.cluster.port = {}", port);
    LOGGER.info("app.cluster.seedNodes = {}", seedNodes);

    Map<String, Object> overrides = new HashMap<>();
    overrides.put("akka.remote.artery.canonical.hostname", hostName);
    overrides.put("akka.remote.artery.canonical.port", port);
    overrides.put("akka.cluster.seed-seedNodes", seedNodes);

    Config config = ConfigFactory.parseMap(overrides)
        .withFallback(ConfigFactory.load("akka-cluster"));

    return ActorSystem.create(MoleculeSourceRootActor.create(properties.maxSearchActors()),
        QDP_AKKA_SYSTEM, config);
  }

}
