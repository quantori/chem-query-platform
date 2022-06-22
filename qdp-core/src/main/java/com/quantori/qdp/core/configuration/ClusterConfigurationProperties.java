package com.quantori.qdp.core.configuration;

import java.util.List;

public record ClusterConfigurationProperties(int maxSearchActors,
                                             String ecsContainerMetadataUri,
                                             String clusterHostName,
                                             int clusterPort,
                                             List<String> seedNodes) {
}
