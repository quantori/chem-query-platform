package com.quantori.cqp.core.configuration;

import java.util.List;
import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class ClusterConfigurationProperties {
  int maxSearchActors;
  String ecsContainerMetadataUri;
  String clusterHostName;
  int clusterPort;
  List<String> seedNodes;
  String systemName;
}
