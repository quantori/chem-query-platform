package com.quantori.cqp.storage.elasticsearch;

import com.quantori.cqp.api.model.Property;
import com.quantori.cqp.storage.elasticsearch.model.MoleculeDocument;
import lombok.experimental.UtilityClass;

import java.util.Map;

@UtilityClass
class ElasticIndexMappingsFactory {

  public static final String PROPERTIES = "properties";
  public static final String MOL_PROPERTIES = MoleculeDocument.FieldMapping.MOL_PROPERTIES.getPropertyName();
  public static final String MOL_PROPERTIES_FIELD = MOL_PROPERTIES + ".";

  public static final String SUB = "sub";
  public static final String EXACT = "exact";

  public static final String ROLE = "role";
  public static final String REACTION_ID = "reactionId";

  public static final Map<Property.PropertyType, String> TYPES_MAP = Map.of(
    Property.PropertyType.STRING, co.elastic.clients.elasticsearch._types.mapping.Property.Kind.Wildcard.jsonValue(),
    Property.PropertyType.DATE, co.elastic.clients.elasticsearch._types.mapping.Property.Kind.Date.jsonValue(),
    Property.PropertyType.DECIMAL, co.elastic.clients.elasticsearch._types.mapping.Property.Kind.Double.jsonValue()
  );

  public static final
  Map<co.elastic.clients.elasticsearch._types.mapping.Property.Kind, Property.PropertyType> KINDS_MAP = Map.of(
    co.elastic.clients.elasticsearch._types.mapping.Property.Kind.Wildcard, Property.PropertyType.STRING,
    co.elastic.clients.elasticsearch._types.mapping.Property.Kind.Date, Property.PropertyType.DATE,
    co.elastic.clients.elasticsearch._types.mapping.Property.Kind.Double, Property.PropertyType.DECIMAL
  );

  public static final String MOLECULES_MAPPING = String.format("""
    {
      "settings": {
        "number_of_shards": %%d,
        "number_of_replicas": %%d
      },
      "mappings": {
        "dynamic_templates": [
          {
            "strings": {
              "match_mapping_type": "string",
              "mapping": {
                "type": "%s"
              }
            }
          },
          {
            "decimals": {
              "match_mapping_type": "double",
              "mapping": {
                "type": "%s"
              }
            }
          },
          {
            "integers": {
              "match_mapping_type": "long",
              "mapping": {
                "type": "%s"
              }
            }
          }
        ],
        "properties": {
          "structure": {
            "type": "binary"
          },
          "sub": {
            "type": "text"
          },
          "sim": {
            "type": "dense_vector",
            "dims": %%s
          },
          "exact": {
            "type": "keyword"
          },
          "molproperties": {
            "type": "nested"
            %%s
          }
        }
      }
    }
    """,
    TYPES_MAP.get(Property.PropertyType.STRING),
    TYPES_MAP.get(Property.PropertyType.DECIMAL),
    TYPES_MAP.get(Property.PropertyType.DECIMAL)
    );

  public static final String MOLECULES_UPDATE_INDEX_MAPPING = """
    {
      "type": "nested"
      %s
    }
    """;

  // not indexed
  public static final String REACTION_PARTICIPANTS_MAPPING = """
    {
      "settings": {
        "number_of_shards": %d,
        "number_of_replicas": %d
      },
      "mappings": {
        "properties": {
          "role": {
            "type": "text"
          },
          "sub": {
            "type": "text"
          },
          "exact": {
            "type": "keyword"
          },
          "reactionId": {
            "type": "text"
          },
          "type": {
            "type": "text"
          },
          "structure": {
            "type": "text",
            "index": false
          },
          "name": {
            "type": "text",
            "index": false
          },
          "inchi": {
            "type": "text",
            "index": "false"
          }
        }
      }
    }
    """;

  //didn't specify _id since it is  indexed by default
  public static final String ACTUAL_REACTION_MAPPING = """
      {
        "settings": {
          "number_of_shards": %d,
          "number_of_replicas": %d
        },
        "mappings": {
          "properties": {
            "reactionSmiles": {
              "type": "text",
              "index": false
            },
            "reactionId": {
              "type": "text",
              "index": false
            },
            "source": {
              "type": "text",
              "index": false
            },
            "description": {
              "type": "text",
              "index": false
            },
            "amount": {
              "type": "text",
              "index": false
            },
            "sub": {
              "type": "text"
            }
          }
        }
    }
    """;

  public static final String LIBRARIES_MAPPING = """
    {
      "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
      },
      "mappings": {
        "properties": {
          "created_timestamp": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "name": {
            "type": "keyword",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "size": {
            "type": "long"
          },
          "type": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "properties_mapping": {
            "type": "object",
            "enabled": false
          }
        }
      }
    }
    """;

  private static final String[] DATE_FORMATS = new String[] {
    "yyyy-MM-dd HH:mm:ss",
    "MM/dd/yyyy HH:mm:ss a",
    "dd.MM.yyyy",
    "dd/MM-yyyy"
  };

  public static final String DATE_PROPERTY_MAPPING = String.format("""
    "%%s": {
      "type": "%%s",
      "ignore_malformed": true,
      "format": "%s"
    }
    """, String.join("||", DATE_FORMATS));

  public static final String PROPERTY_MAPPING = """
    "%s": {
      "type": "%s"
    }
    """;
}
