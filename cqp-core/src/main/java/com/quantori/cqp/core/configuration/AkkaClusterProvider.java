package com.quantori.cqp.core.configuration;

import akka.actor.typed.ActorSystem;
import com.quantori.cqp.core.source.SourceRootActor;
import org.apache.commons.lang3.StringUtils;

public interface AkkaClusterProvider {
  String CQP_AKKA_SYSTEM = "cqp-akka-system";

  ActorSystem<SourceRootActor.Command> actorTypedSystem(ClusterConfigurationProperties properties);

  default akka.actor.ActorSystem actorSystem(
      ClusterConfigurationProperties properties, Runnable callback) {
    akka.actor.ActorSystem system = actorTypedSystem(properties).classicSystem();
    system.registerOnTermination(callback);
    return system;
  }

  default String getSystemNameOrDefault(String clusterName) {
    if (StringUtils.isNotEmpty(clusterName)) {
      return clusterName;
    } else {
      return CQP_AKKA_SYSTEM;
    }
  }
}
