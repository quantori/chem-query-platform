package com.quantori.qdp.core.configuration;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Builder
@Data
public class ClusterConfigurationProperties {
    int maxSearchActors;
    String ecsContainerMetadataUri;
    String clusterHostName;
    int clusterPort;
    List<String> seedNodes;
}
