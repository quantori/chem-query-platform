package com.quantori.qdp.core.configuration;

import static com.quantori.qdp.core.configuration.ClusterProvider.QDP_AKKA_SYSTEM;

import akka.actor.typed.ActorSystem;
import com.quantori.qdp.core.source.MoleculeSourceRootActor;

public class SimpleClusterProvider implements ClusterProvider {

  @Override
  public ActorSystem<MoleculeSourceRootActor.Command> actorTypedSystem(ClusterConfigurationProperties properties) {
    return ActorSystem.create(MoleculeSourceRootActor.create(properties.maxSearchActors()),
        QDP_AKKA_SYSTEM);
  }

}
