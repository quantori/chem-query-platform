package com.quantori.cqp.storage.elasticsearch;

import com.quantori.cqp.api.model.Property;
import com.quantori.cqp.storage.elasticsearch.model.MoleculeDocument;
import lombok.experimental.UtilityClass;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Map.entry;

@UtilityClass
class ElasticIndexMappingsFactory {

  public static final String PROPERTIES = "properties";
  public static final String MOL_PROPERTIES = MoleculeDocument.FieldMapping.MOL_PROPERTIES.getPropertyName();
  public static final String MOL_PROPERTIES_FIELD = MOL_PROPERTIES + ".";

  public static final String SUB = "sub";
  public static final String EXACT = "exact";

  public static final String ROLE = "role";
  public static final String REACTION_ID = "reactionId";

  public static final String PROPERTY_TYPE_META_KEY = "propertyType";

  private static final Map<Property.PropertyType,
    co.elastic.clients.elasticsearch._types.mapping.Property.Kind> KIND_BY_PROPERTY_TYPE = Map.ofEntries(
    entry(Property.PropertyType.STRING, co.elastic.clients.elasticsearch._types.mapping.Property.Kind.Wildcard),
    entry(Property.PropertyType.DATE, co.elastic.clients.elasticsearch._types.mapping.Property.Kind.Date),
    entry(Property.PropertyType.DATE_TIME, co.elastic.clients.elasticsearch._types.mapping.Property.Kind.DateNanos),
    entry(Property.PropertyType.DECIMAL, co.elastic.clients.elasticsearch._types.mapping.Property.Kind.Double),
    entry(Property.PropertyType.BINARY, co.elastic.clients.elasticsearch._types.mapping.Property.Kind.Binary),
    entry(Property.PropertyType.LIST, co.elastic.clients.elasticsearch._types.mapping.Property.Kind.Keyword),
    entry(Property.PropertyType.HYPERLINK, co.elastic.clients.elasticsearch._types.mapping.Property.Kind.MatchOnlyText),
    entry(Property.PropertyType.CHEMICAL_STRUCTURE,
      co.elastic.clients.elasticsearch._types.mapping.Property.Kind.Text),
    entry(Property.PropertyType.STRUCTURE_3D,
      co.elastic.clients.elasticsearch._types.mapping.Property.Kind.SearchAsYouType),
    entry(Property.PropertyType.HTML, co.elastic.clients.elasticsearch._types.mapping.Property.Kind.Text)
  );

  public static final Map<Property.PropertyType, String> TYPES_MAP =
    KIND_BY_PROPERTY_TYPE.entrySet().stream()
      .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> entry.getValue().jsonValue()));

  public static final Map<co.elastic.clients.elasticsearch._types.mapping.Property.Kind, Property.PropertyType>
    KINDS_MAP;

  static {
    EnumMap<co.elastic.clients.elasticsearch._types.mapping.Property.Kind, Property.PropertyType> map =
      new EnumMap<>(co.elastic.clients.elasticsearch._types.mapping.Property.Kind.class);
    KIND_BY_PROPERTY_TYPE.forEach((propertyType, kind) -> map.putIfAbsent(kind, propertyType));
    KINDS_MAP = Collections.unmodifiableMap(map);
  }

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
    "dd/MM-yyyy",
    "strict_date_optional_time",
    "strict_date_optional_time_nanos"
  };

  public static final String DATE_FORMAT_PATTERN = String.join("||", DATE_FORMATS);

  public static final String DATE_PROPERTY_MAPPING = """
    "%s": {
      "type": "%s",
      "ignore_malformed": true,
      "format": "%s",
      "meta": {
        "%s": "%s"
      }
    }
    """;

  public static final String PROPERTY_MAPPING = """
    "%s": {
      "type": "%s",
      "meta": {
        "%s": "%s"
      }
    }
    """;
}
