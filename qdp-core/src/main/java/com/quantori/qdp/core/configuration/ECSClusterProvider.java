package com.quantori.qdp.core.configuration;

import akka.actor.typed.ActorSystem;
import akka.management.cluster.bootstrap.ClusterBootstrap;
import akka.management.javadsl.AkkaManagement;
import com.quantori.qdp.core.source.MoleculeSourceRootActor;
import com.quantori.qdp.core.utilities.ECSConfigurationProvider;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ECSClusterProvider implements AkkaClusterProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(ECSClusterProvider.class);

  @Override
  public ActorSystem<MoleculeSourceRootActor.Command> actorTypedSystem(ClusterConfigurationProperties properties) {
    String metadataUri = properties.ecsContainerMetadataUri();
    Config config = ConfigFactory.parseMap(ECSConfigurationProvider.getConfiguration(metadataUri))
        .withFallback(ConfigFactory.load("akka-cluster-ecs"));
    var akkaSystem =
        ActorSystem.create(MoleculeSourceRootActor.create(properties.maxSearchActors()),
            QDP_AKKA_SYSTEM,
            config);

    AkkaManagement.get(akkaSystem).start();
    ClusterBootstrap.get(akkaSystem).start();
    return akkaSystem;
  }

}
