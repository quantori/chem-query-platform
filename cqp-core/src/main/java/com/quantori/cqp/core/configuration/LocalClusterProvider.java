package com.quantori.cqp.core.configuration;

import akka.actor.typed.ActorSystem;
import com.quantori.cqp.core.source.SourceRootActor;

public class LocalClusterProvider implements AkkaClusterProvider {

  @Override
  public ActorSystem<SourceRootActor.Command> actorTypedSystem(
      ClusterConfigurationProperties properties) {
    return ActorSystem.create(
        SourceRootActor.create(properties.getMaxSearchActors()),
        getSystemNameOrDefault(properties.getSystemName()));
  }
}
