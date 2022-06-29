package com.quantori.qdp.core.configuration;

import akka.actor.typed.ActorSystem;
import com.quantori.qdp.core.source.MoleculeSourceRootActor;

public interface AkkaClusterProvider {
  String QDP_AKKA_SYSTEM = "qdp-akka-system";

  ActorSystem<MoleculeSourceRootActor.Command> actorTypedSystem(
      ClusterConfigurationProperties properties);

  default akka.actor.ActorSystem actorSystem(ClusterConfigurationProperties properties, Runnable callback) {
    akka.actor.ActorSystem system = actorTypedSystem(properties).classicSystem();
    system.registerOnTermination(callback);
    return system;
  }
}
