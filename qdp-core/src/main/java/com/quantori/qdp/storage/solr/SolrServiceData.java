package com.quantori.qdp.storage.solr;

import java.util.Map;
import javax.validation.constraints.Min;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.solr.common.annotation.JsonProperty;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class SolrServiceData {
  public static final String PROPERTIES = "properties";
  public static final String REPLICAS_PROP = "replicas";
  public static final String SHARDS_PROP = "shards";
  public static final String REPLICA_TYPE = "replica_type";
  private static final int DEFAULT_SHARDS = 1;
  private static final int DEFAULT_REPLICAS = 1;

  @JsonProperty(defaultValue = "1")
  @Min(1)
  private int shards = DEFAULT_SHARDS;

  @JsonProperty(defaultValue = "1")
  @Min(1)
  private int replicas = DEFAULT_REPLICAS;

  private ReplicaType replicaType = ReplicaType.NRT;

  public Map<String, Object> toMap() {
    return Map.of(SHARDS_PROP, shards, REPLICAS_PROP, replicas, REPLICA_TYPE, replicaType);
  }

  enum ReplicaType {
    NRT, TLOG, PULL
  }
}
