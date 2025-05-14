package com.quantori.cqp.storage.elasticsearch;

import co.elastic.clients.elasticsearch._types.Script;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.ChildScoreMode;
import co.elastic.clients.elasticsearch._types.query_dsl.ExistsQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.RegexpQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.ScriptScoreQuery;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.transport.ElasticsearchTransport;
import com.quantori.cqp.api.MoleculesFingerprintCalculator;
import com.quantori.cqp.api.MoleculesMatcher;
import com.quantori.cqp.api.StorageMolecules;
import com.quantori.cqp.api.model.ConjunctionCriteria;
import com.quantori.cqp.api.model.FieldCriteria;
import com.quantori.cqp.api.model.Flattened;
import com.quantori.cqp.api.model.Library;
import com.quantori.cqp.api.model.Property;
import com.quantori.cqp.api.model.upload.Molecule;
import com.quantori.cqp.api.util.FingerPrintUtilities;
import com.quantori.cqp.core.model.Criteria;
import com.quantori.cqp.core.model.ExactParams;
import com.quantori.cqp.core.model.ItemWriter;
import com.quantori.cqp.core.model.SearchIterator;
import com.quantori.cqp.core.model.SearchProperty;
import com.quantori.cqp.core.model.SimilarityParams;
import com.quantori.cqp.core.model.SortParams;
import com.quantori.cqp.core.model.SubstructureParams;
import com.quantori.cqp.storage.elasticsearch.model.MoleculeDocument;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
class ElasticsearchStorageMolecules extends ElasticsearchStorageBase implements StorageMolecules {

  // it would be really nice to find existing elasticsearch-java functionality to escape values
  private static final String SPECIAL_CHARACTERS_ESCAPE_PATTERN = "([+\\-!(){}\\[\\]^\"~*?:\\\\]|[&|]{2})";

  private final MoleculesMatcher moleculesMatcher;
  private final ElasticsearchStoragePropertiesMapping storagePropertiesMapping;
  private final MoleculesFingerprintCalculator moleculesFingerprintCalculator;

  public ElasticsearchStorageMolecules(
      ElasticsearchTransport transport, ElasticsearchProperties properties,
      ElasticsearchStorageLibrary storageLibrary,
      ElasticsearchStoragePropertiesMapping storagePropertiesMapping,
      MoleculesMatcher moleculesMatcher,
      MoleculesFingerprintCalculator moleculesFingerprintCalculator
  ) {
    super(transport, properties, storageLibrary);
    this.moleculesMatcher = moleculesMatcher;
    this.storagePropertiesMapping = storagePropertiesMapping;
    this.moleculesFingerprintCalculator = moleculesFingerprintCalculator;
  }

  @Override
  public MoleculesFingerprintCalculator fingerPrintCalculator() {
    return moleculesFingerprintCalculator;
  }

  @Override
  public SearchIterator<Flattened.Molecule> searchExact(String[] libraryIds, byte[] moleculeExactFingerprint,
                                                        List<SearchProperty> properties, ExactParams exactParams,
                                                        SortParams sortParams, Criteria criteria) {
    var matchQuery = QueryBuilders.match(mq -> mq.field(ElasticIndexMappingsFactory.EXACT)
      .query(FingerPrintUtilities.exactHash(moleculeExactFingerprint)));

    Query query = QueryBuilders.bool()
      .filter(matchQuery)
      .filter(queryBuilder(libraryIds, properties)._toQuery())
      .filter(queryBuilder(libraryIds, criteria)._toQuery())
      .build()
      ._toQuery();

    return searchForMolecules(
      libraryIds,
      getSearchRequestBuilder(query, getSortOptions(libraryIds, sortParams)),
      molecule -> moleculesMatcher.isExactMatch(molecule.getStructure(), exactParams),
      Arrays.asList(libraryIds)
    );
  }

  @Override
  public SearchIterator<Flattened.Molecule> searchSub(String[] libraryIds, byte[] queryFingerprint,
                                                      List<SearchProperty> properties,
                                                      SubstructureParams substructureParams, SortParams sortParams,
                                                      Criteria criteria) {
    var query = searchStructureBuilder(libraryIds, properties, criteria, queryFingerprint).build()._toQuery();
    return searchForMolecules(
      libraryIds,
      getSearchRequestBuilder(query, getSortOptions(libraryIds, sortParams)),
      molecule -> moleculesMatcher.isSubstructureMatch(molecule.getStructure(), substructureParams),
      Arrays.asList(libraryIds)
    );
  }

  @Override
  public SearchIterator<Flattened.Molecule> searchSim(String[] libraryIds, byte[] moleculeSimilarityFingerprint,
                                                      List<SearchProperty> properties,
                                                      SimilarityParams similarityParams, SortParams sortParams,
                                                      Criteria criteria) {
    List<Double> similarityHash = ElasticsearchFingerprintUtilities.getSimilarityHash(moleculeSimilarityFingerprint);
    Map<String, JsonData> similarityParamsMap = Map.of(
      "sim", JsonData.of(similarityHash),
      "zero", JsonData.of(ElasticsearchFingerprintUtilities.ZERO_LIST),
      "nonZero", JsonData.of(ElasticsearchFingerprintUtilities.numberOfNonZeroElements(similarityHash)),
      "alpha", JsonData.of(similarityParams.getAlpha()),
      "beta", JsonData.of(similarityParams.getBeta()),
      "min", JsonData.of(similarityParams.getMinSim()),
      "max", JsonData.of(similarityParams.getMaxSim())
    );

    Script script = new Script.Builder()
      .stored(x -> x.id(similarityParams.getMetric().getValue())
        .params(similarityParamsMap)).build();

    ScriptScoreQuery builder = new ScriptScoreQuery.Builder()
      .query(q -> q.bool(b -> b.filter(queryBuilder(libraryIds, properties)._toQuery())))
      .query(q -> q.bool(b -> b.filter(queryBuilder(libraryIds, criteria)._toQuery())))
      .script(script).build();

    return searchForMolecules(
      libraryIds,
      getSearchRequestBuilder(builder._toQuery(), getSortOptions(libraryIds, sortParams), similarityParams.getMinSim()),
      molecule -> true,
      Arrays.asList(libraryIds)
    );
  }

  @Override
  public List<Flattened.Molecule> findById(String libraryId, String... moleculeIds) {
    var libraryOptional = storageLibrary.getLibraryById(libraryId);
    if (libraryOptional.isEmpty()) {
      return List.of();
    }
    var request = new SearchRequest.Builder().index(libraryId)
      .query(QueryBuilders.ids(e -> e.values(List.of(moleculeIds))))
      .size(moleculeIds.length).build();
    try {
      var response = client.search(request, MoleculeDocument.class);
      var moleculeMap = getMoleculesFromSearchHits(response.hits().hits(), List.of(libraryOptional.get())).stream()
        .collect(Collectors.toMap(Flattened.Molecule::getId, Function.identity()));

      return Arrays.stream(moleculeIds)
        .map(moleculeMap::get)
        .filter(Objects::nonNull)
        .toList();
    } catch (IllegalStateException e) {
      throw new ElasticsearchStorageException("Duplicate molecules found", e);
    } catch (IOException e) {
      throw new ElasticsearchStorageException("Failed to get results from elasticsearch", e);
    }
  }

  @Override
  public ItemWriter<Molecule> itemWriter(String libraryId) {
    return new ElasticsearchMoleculeUploader(asyncClient, libraryId, fingerPrintCalculator(),
      this::updateLibraryDocument);
  }

  @Override
  public long countElements(String libraryId) {
    return super.countElements(libraryId);
  }

  @Override
  public boolean deleteMolecules(String libraryId, List<String> moleculeIds) {
    return super.deleteItemsAndUpdateLibrarySize(libraryId, moleculeIds);
  }

  private List<SortOptions> getSortOptions(String[] libraryIds, SortParams sortParams) {
    if (sortParams == null || sortParams.sortList() == null || sortParams.sortList().isEmpty()) {
      return List.of();
    }

    // fetch properties only if needed
    Map<String, Map<String, Property>> propertiesMapping;
    boolean hasNestedProperties = sortParams.sortList()
      .stream()
      .anyMatch(sort -> SortParams.Type.NESTED == sort.type());
    if (hasNestedProperties) {
      propertiesMapping = storageLibrary.getPropertiesMapping(libraryIds);
    } else {
      propertiesMapping = null;
    }

    return sortParams.sortList().stream()
      .map(sort -> {
        if (SortParams.Type.NESTED == sort.type()) {
          assert propertiesMapping != null;
          Optional<String> fieldName =
            storagePropertiesMapping.getPropertyFieldName(sort.fieldName(), propertiesMapping);
          if (fieldName.isEmpty()) {
            return null;
          }
          return SortOptions.of(
            sortOptionsBuilder -> sortOptionsBuilder
              .field(
                fieldSortBuilder -> fieldSortBuilder.nested(
                    nestedSortBuilder -> nestedSortBuilder.path(ElasticIndexMappingsFactory.MOL_PROPERTIES))
                  .field(fieldName.get())
                  .order(getAscDescOrder(sort))));
        } else {
          Optional<String> fieldName = Mapper.elasticMoleculeFieldName(sort.fieldName());
          if (fieldName.isEmpty()) {
            return null;
          }
          return SortOptions.of(
            sortOptionsBuilder -> sortOptionsBuilder
              .field(fieldSortBuilder -> fieldSortBuilder
                .field(fieldName.get())
                .order(getAscDescOrder(sort))));
        }
      })
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
  }

  private SortOrder getAscDescOrder(SortParams.Sort sort) {
    if (sort == null || sort.order() == null || sort.order() != SortParams.Order.DESC) {
      return SortOrder.Asc;
    }
    return SortOrder.Desc;
  }

  BoolQuery.Builder searchStructureBuilder(String[] libraryIds, List<SearchProperty> properties, Criteria criteria,
                                           byte[] queryFingerprint) {
    var sub = searchStructureBuilder(queryFingerprint);
    sub.filter(queryBuilder(libraryIds, properties)._toQuery());
    sub.filter(queryBuilder(libraryIds, criteria)._toQuery());
    return sub;
  }

  // left for backward compatibility with properties api, once all storages implement criteria api, this can be removed
  BoolQuery queryBuilder(String[] libraryIds, List<SearchProperty> properties) {
    Criteria criteria;
    if (properties == null) {
      criteria = null;
    } else {
      Function<String, FieldCriteria.Operator> toOperator = stringOperator -> switch (stringOperator) {
        case ">" -> FieldCriteria.Operator.GREATER_THAN;
        case "<" -> FieldCriteria.Operator.LESS_THAN;
        case "=" -> FieldCriteria.Operator.EQUAL;
        case ">=" -> FieldCriteria.Operator.GREATER_THAN_OR_EQUAL;
        case "<=" -> FieldCriteria.Operator.LESS_THAN_OR_EQUAL;
        default -> throw new IllegalArgumentException("Unknown logical operator " + stringOperator);
      };

      List<FieldCriteria> fieldCriteriaList = properties.stream()
        .map(property ->
          new FieldCriteria(property.getProperty(), toOperator.apply(property.getLogicalOperator()),
            property.getValue()))
        .toList();
      if (fieldCriteriaList.isEmpty()) {
        criteria = null;
      } else if (fieldCriteriaList.size() == 1) {
        criteria = fieldCriteriaList.get(0);
      } else {
        criteria = new ConjunctionCriteria(fieldCriteriaList, ConjunctionCriteria.DEFAULT_OPERATOR);
      }
    }
    return queryBuilder(libraryIds, criteria);
  }

  BoolQuery queryBuilder(String[] libraryIds, Criteria criteria) {
    var bq = QueryBuilders.bool();
    if (criteria != null) {
      Map<String, Map<String, Property>> propertiesMapping = storageLibrary.getPropertiesMapping(libraryIds);
      subQuery(criteria, propertiesMapping).ifPresent(subQuery ->
        bq.filter(QueryBuilders.nested(
          n -> n.path(ElasticIndexMappingsFactory.MOL_PROPERTIES).query(subQuery).scoreMode(ChildScoreMode.None))));
    }
    return bq.build();
  }

  Optional<Query> subQuery(Criteria criteria, Map<String, Map<String, Property>> propertiesMapping) {
    if (criteria instanceof ConjunctionCriteria conjunctionCriteria) {
      return subQuery(conjunctionCriteria, propertiesMapping);
    } else if (criteria instanceof FieldCriteria fieldCriteria) {
      return subQuery(fieldCriteria, propertiesMapping);
    }
    return Optional.empty();
  }

  Optional<Query> subQuery(ConjunctionCriteria criteria, Map<String, Map<String, Property>> propertiesMapping) {
    if (criteria == null || criteria.getCriteriaList() == null) {
      return Optional.empty();
    }
    List<Query> criteriaList = criteria.getCriteriaList().stream()
      .filter(Objects::nonNull)
      .map(c -> subQuery(c, propertiesMapping))
      .flatMap(Optional::stream)
      .toList();
    if (criteriaList.isEmpty()) {
      return Optional.empty();
    }
    ConjunctionCriteria.Operator operator =
      Objects.requireNonNullElse(criteria.getOperator(), ConjunctionCriteria.Operator.AND);
    var subQuery = switch (operator) {
      case OR -> QueryBuilders.bool().should(criteriaList);
      case AND -> QueryBuilders.bool().must(criteriaList);
    };
    return Optional.of(subQuery.build()._toQuery());
  }

  Optional<Query> subQuery(FieldCriteria criteria, Map<String, Map<String, Property>> propertiesMapping) {
    if (criteria == null || StringUtils.isBlank(criteria.getFieldName()) || criteria.getOperator() == null) {
      return Optional.empty();
    }
    var fieldTypeOptional = storagePropertiesMapping.getPropertyType(criteria.getFieldName(), propertiesMapping);
    var fieldName = ElasticIndexMappingsFactory.MOL_PROPERTIES_FIELD + criteria.getFieldName();
    Query subQuery = switch (criteria.getOperator()) {
      case NONEMPTY, EMPTY -> {
        var fieldType = fieldTypeOptional.orElse(null);
        var boolQuery = new BoolQuery.Builder();
        if (FieldCriteria.Operator.NONEMPTY == criteria.getOperator()) {
          boolQuery.must(getPropertyExistsQuery(fieldName, fieldType));
        } else {
          boolQuery.mustNot(getPropertyExistsQuery(fieldName, fieldType));
        }
        yield boolQuery.build()._toQuery();
      }
      default -> {
        // field is allowed to be missing for empty / nonempty filtering only
        if (fieldTypeOptional.isEmpty()) {
          yield null;
        }
        var fieldType = fieldTypeOptional.get();
        var convertedValue = convert(criteria.getValue(), fieldType);
        yield switch (criteria.getOperator()) {
          case LESS_THAN -> QueryBuilders.range(q -> q.field(fieldName).lt(JsonData.of(convertedValue)));
          case LESS_THAN_OR_EQUAL -> QueryBuilders.range(q -> q.field(fieldName).lte(JsonData.of(convertedValue)));
          case GREATER_THAN -> QueryBuilders.range(q -> q.field(fieldName).gt(JsonData.of(convertedValue)));
          case GREATER_THAN_OR_EQUAL -> QueryBuilders.range(q -> q.field(fieldName).gte(JsonData.of(convertedValue)));
          case EQUAL -> fieldType == Property.PropertyType.STRING
            ? QueryBuilders.match(mq -> mq.field(fieldName).query(convertedValue.toString()))
            : QueryBuilders.term(tq -> tq.field(fieldName).value(x -> x.anyValue(JsonData.of(convertedValue))));
          case NOT_EQUAL -> fieldType == Property.PropertyType.STRING
            ? QueryBuilders.bool()
                  .mustNot(mq -> mq.match(m -> m.field(fieldName).query(convertedValue.toString()))).build()._toQuery()
            : QueryBuilders.bool()
                  .mustNot(mn -> mn.term(tq -> tq.field(fieldName).value(x -> x.anyValue(JsonData.of(convertedValue))))).build()._toQuery();
          case CONTAIN -> QueryBuilders.wildcard(
            wq -> wq.field(fieldName).wildcard("*%s*".formatted(escape(convertedValue.toString())))
              .caseInsensitive(true));
          // already processed
          case NOT_CONTAIN -> QueryBuilders
                  .bool(mn -> mn.mustNot(qb -> qb.wildcard(wq -> wq.field(fieldName).wildcard("*%s*".formatted(escape(convertedValue.toString())))
                  .caseInsensitive(true))));
          case NONEMPTY, EMPTY -> throw new IllegalStateException();
        };
      }
    };
    return Optional.ofNullable(subQuery);
  }

  private Query getPropertyExistsQuery(String fieldName, Property.PropertyType type) {
    if (type == null) {
      return new ExistsQuery.Builder().field(fieldName)
        .build()._toQuery();
    }
    return switch (type) {
      case DECIMAL -> new RangeQuery.Builder().field(fieldName)
        .lte(JsonData.of(Long.MAX_VALUE))
        .gte(JsonData.of(Long.MIN_VALUE))
        .build()._toQuery();
      case STRING -> new RegexpQuery.Builder().field(fieldName).value(".+")
        .build()._toQuery();
      default -> new ExistsQuery.Builder().field(fieldName)
        .build()._toQuery();
    };
  }

  private List<Flattened.Molecule> getMoleculesFromSearchHits(List<Hit<MoleculeDocument>> hits,
                                                              List<Library> libraries) {
    return getMoleculesFromSearchHits(hits, libraries, molecule -> true);
  }

  private List<Flattened.Molecule> getMoleculesFromSearchHits(List<Hit<MoleculeDocument>> hits,
                                                              List<Library> libraries,
                                                              Predicate<Flattened.Molecule> filter) {
    return hits.stream()
      .map(hit -> tryToGetMolecule(hit, libraries))
      .flatMap(Optional::stream)
      .filter(filter)
      .toList();
  }

  private Optional<Flattened.Molecule> tryToGetMolecule(Hit<MoleculeDocument> hit, List<Library> libraries) {
    String json = JsonMapper.objectToJSON(hit.source());
    try {
      var index = hit.index();
      var moleculeDocument = JsonMapper.toMoleculeDocument(json);
      var molecule = Mapper.flattenMolecule(hit.id(), index, moleculeDocument, libraries);
      return Optional.of(molecule);
    } catch (IOException e) {
      log.error("Failed to get molecule {}", json, e);
      return Optional.empty();
    }
  }

  private SearchIterator<Flattened.Molecule> searchForMolecules(
    String[] indexNames, SearchRequest.Builder builder, Predicate<Flattened.Molecule> filter, List<String> libraryIds) {
    builder.index(List.of(indexNames)).requestCache(false)
      .scroll(timeValue(properties.getScrollTimeout()));
    if (properties.getMaxConcurrentShardRequests() > 0) {
      builder.maxConcurrentShardRequests((long) properties.getMaxConcurrentShardRequests());
    }
    List<Library> libraries = storageLibrary.getLibraryById(libraryIds);
    return new ElasticsearchIterator.Molecules(
      transport,
      builder.build(),
      timeValue(properties.getScrollTimeout()),
      hits -> getMoleculesFromSearchHits(hits, libraries, filter),
      libraryIds
    );
  }

  private String escape(String value) {
    return value.replaceAll(SPECIAL_CHARACTERS_ESCAPE_PATTERN, "\\\\$1");
  }

  private Object convert(String value, Property.PropertyType type) {
    return switch (type) {
      case DATE -> {
        // TODO limited date support, needs to add a check for formats in future
        if (StringUtils.isBlank(value)) {
          throw new ElasticsearchStorageException(String.format("A value \"%s\" cannot be cast to date", value));
        } else {
          yield value;
        }
      }
      case STRING -> Objects.requireNonNullElse(value, "");
      case DECIMAL -> {
        if (NumberUtils.isParsable(value)) {
          yield new BigDecimal(value);
        } else {
          throw new ElasticsearchStorageException(String.format("A value \"%s\" cannot be cast to decimal", value));
        }
      }
    };
  }
}
