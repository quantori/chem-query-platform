package com.quantori.qdp.core.configuration;

import akka.actor.typed.ActorSystem;
import com.quantori.qdp.core.source.MoleculeSourceRootActor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterProvider implements AkkaClusterProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterProvider.class);

  @Override
  public ActorSystem<MoleculeSourceRootActor.Command> actorTypedSystem(ClusterConfigurationProperties properties) {
    String hostName = properties.clusterHostName();
    int port = properties.clusterPort();
    List<String> seedNodes = getSeedNodes(properties.seedNodes());

    LOGGER.info("app.cluster.hostname = {}", hostName);
    LOGGER.info("app.cluster.port = {}", port);
    LOGGER.info("app.cluster.nodes = {}", seedNodes);

    Map<String, Object> overrides = new HashMap<>();
    overrides.put("akka.remote.artery.canonical.hostname", hostName);
    overrides.put("akka.remote.artery.canonical.port", port);
    overrides.put("akka.cluster.seed-nodes", seedNodes);

    Config config = ConfigFactory.parseMap(overrides)
        .withFallback(ConfigFactory.load("akka-cluster"));

    return ActorSystem.create(MoleculeSourceRootActor.create(properties.maxSearchActors()),
        QDP_AKKA_SYSTEM, config);
  }

  private List<String> getSeedNodes(List<String> nodes) {
    return nodes.stream()
        .map(String::trim)
        .filter(StringUtils::isNoneBlank)
        .map(n -> {
          if (n.startsWith("akka:")) {
            return n;
          } else {
            return "akka://" + QDP_AKKA_SYSTEM + "@" + n;
          }
        }).toList();
  }

}
