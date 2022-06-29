package com.quantori.qdp.core.configuration;

import akka.actor.typed.ActorSystem;
import com.quantori.qdp.core.source.MoleculeSourceRootActor;

public class LocalClusterProvider implements AkkaClusterProvider {

  @Override
  public ActorSystem<MoleculeSourceRootActor.Command> actorTypedSystem(ClusterConfigurationProperties properties) {
    return ActorSystem.create(MoleculeSourceRootActor.create(properties.getMaxSearchActors()),
        QDP_AKKA_SYSTEM);
  }

}
