package com.quantori.cqp.storage.elasticsearch;

import com.quantori.cqp.api.model.MapConvertable;
import lombok.Getter;
import lombok.experimental.FieldNameConstants;

import java.util.Map;

@Getter
@FieldNameConstants
public class ElasticsearchServiceData implements MapConvertable {
  protected int shards = 1;
  protected int replicas = 0;

  public Map<String, Object> toMap() {
    return Map.of(Fields.shards, shards, Fields.replicas, replicas);
  }

}
