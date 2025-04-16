package com.quantori.cqp.storage.elasticsearch.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.experimental.FieldNameConstants;

import java.util.List;
import java.util.Map;

@Data
@EqualsAndHashCode(callSuper = true)
@FieldNameConstants
public class MoleculeDocument extends BasicMoleculeDocument {

  @JsonProperty(ElasticPropertyNames.SIM)
  private List<Double> simHash;

  @JsonProperty(ElasticPropertyNames.MOL_PROPERTIES)
  private Map<String, Object> molProperties;

  @JsonProperty(ElasticPropertyNames.CUSTOM_ORDER)
  private Long customOrder;

  @JsonProperty(ElasticPropertyNames.CREATED_TIMESTAMP)
  private Long createdStamp;

  @JsonProperty(ElasticPropertyNames.UPDATED_TIMESTAMP)
  private Long updatedStamp;

  public static final String TEST = Fields.simHash;

  public enum FieldMapping {
    SIM(Fields.simHash, ElasticPropertyNames.SIM),
    EXACT(BasicMoleculeDocument.Fields.exactHash, ElasticPropertyNames.EXACT),
    MOL_PROPERTIES(Fields.molProperties, ElasticPropertyNames.MOL_PROPERTIES),
    CUSTOM_ORDER(Fields.customOrder, ElasticPropertyNames.CUSTOM_ORDER),
    CREATED_TIMESTAMP(Fields.createdStamp, ElasticPropertyNames.CREATED_TIMESTAMP),
    UPDATED_TIMESTAMP(Fields.updatedStamp, ElasticPropertyNames.UPDATED_TIMESTAMP);

    @Getter
    private final String propertyName;
    @Getter
    private final String elasticName;

    FieldMapping(String propertyName, String elasticName) {
      this.propertyName = propertyName.toLowerCase();
      this.elasticName = elasticName;
    }
  }

  private static class ElasticPropertyNames {
    private static final String SIM = "sim";
    private static final String EXACT = "exact";
    private static final String MOL_PROPERTIES = "molproperties";
    private static final String CUSTOM_ORDER = "custom_order";
    private static final String CREATED_TIMESTAMP = "created_timestamp";
    private static final String UPDATED_TIMESTAMP = "updated_timestamp";
  }
}
