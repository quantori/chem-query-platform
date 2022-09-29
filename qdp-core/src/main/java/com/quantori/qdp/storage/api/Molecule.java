package com.quantori.qdp.storage.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * Generic definition of molecule structure.
 */
@Data
@NoArgsConstructor
public class Molecule {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  @JsonProperty("_id")
  private String id;

  @JsonProperty("structure")
  private String structure;

  @JsonProperty("sub")
  private byte[] sub;

  @JsonProperty("sim")
  private byte[] sim;

  @JsonProperty("exact")
  private byte[] exact;

  @JsonProperty("molproperties")
  private Map<String, String> molProperties;

  @Accessors(chain = true)
  @JsonProperty("name")
  private String name;

  @Accessors(chain = true)
  @JsonProperty("inchi")
  private String inchi;

}
