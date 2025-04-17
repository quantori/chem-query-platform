package com.quantori.cqp.storage.elasticsearch;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryBuilders;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.transport.ElasticsearchTransport;
import com.quantori.cqp.api.model.ExactParams;
import com.quantori.cqp.api.model.Flattened;
import com.quantori.cqp.api.model.Library;
import com.quantori.cqp.api.model.ReactionParticipantRole;
import com.quantori.cqp.api.model.SubstructureParams;
import com.quantori.cqp.api.model.upload.ReactionUploadDocument;
import com.quantori.cqp.api.service.ItemWriter;
import com.quantori.cqp.api.service.ReactionsFingerprintCalculator;
import com.quantori.cqp.api.service.ReactionsMatcher;
import com.quantori.cqp.api.service.SearchIterator;
import com.quantori.cqp.api.service.StorageReactions;
import com.quantori.cqp.api.util.FingerPrintUtilities;
import com.quantori.cqp.storage.elasticsearch.model.ReactionDocument;
import com.quantori.cqp.storage.elasticsearch.model.ReactionParticipantDocument;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RequestOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
class ElasticsearchStorageReactions extends ElasticsearchStorageBase implements StorageReactions {
  private static final String REACTIONS_STORE_PREFIX = "reactions_";
  private static final String REACTION_PARTICIPANTS_STORE_PREFIX = "reactions_participants_";
  private static final int REACTION_PARTICIPANTS_FETCH_SIZE = 100;

  private final ReactionsMatcher reactionsMatcher;
  private final ReactionsFingerprintCalculator reactionsFingerprintCalculator;

  public ElasticsearchStorageReactions(
      ElasticsearchTransport transport, ElasticsearchProperties properties,
      ElasticsearchStorageLibrary storageLibrary, ReactionsMatcher reactionsMatcher,
      ReactionsFingerprintCalculator reactionsFingerprintCalculator
  ) {
    super(transport, properties, storageLibrary);
    this.reactionsMatcher = reactionsMatcher;
    this.reactionsFingerprintCalculator = reactionsFingerprintCalculator;
  }

  public static String getReactionIndexName(String libraryId) {
    return REACTIONS_STORE_PREFIX + libraryId;
  }

  public static String getReactionParticipantIndexName(String libraryId) {
    return REACTION_PARTICIPANTS_STORE_PREFIX + libraryId;
  }

  @Override
  public ReactionsFingerprintCalculator fingerPrintCalculator() {
    return reactionsFingerprintCalculator;
  }

  @Override
  public List<Flattened.Reaction> findById(String libraryId, String... reactionIds) {
    return findByIds(new String[] {libraryId}, reactionIds);
  }

  @Override
  public List<Flattened.ReactionParticipant> searchParticipantsByReactionId(String libraryId, String... reactionIds) {
    var indexNames = getReactionParticipantIndexNames(new String[] {libraryId});
    var queryBuilder = QueryBuilders.bool();
    Arrays.stream(reactionIds)
      .forEach(reactionId -> queryBuilder.should(
        QueryBuilders.match(m -> m.field(ElasticIndexMappingsFactory.REACTION_ID).query(reactionId))));
    var requestBuilder = new SearchRequest.Builder().index(indexNames)
      .requestCache(false)
      .query(queryBuilder.build()._toQuery()).size(REACTION_PARTICIPANTS_FETCH_SIZE)
      .scroll(timeValue(properties.getScrollTimeout()));
    var iterator = new ElasticsearchIterator.ReactionParticipants(
      transport,
      requestBuilder.build(),
      timeValue(properties.getScrollTimeout()),
      this::getReactionParticipantsFromSearchHits,
      List.of(libraryId)
    );
    return scrollIterator(iterator);
  }

  @Override
  public SearchIterator<Flattened.Reaction> searchReactions(String[] libraryIds, byte[] reactionSubstructureFingerprint,
                                                            SubstructureParams substructureParams) {
    var query = searchStructureBuilder(reactionSubstructureFingerprint).build();
    return searchForReactions(
      libraryIds,
      query,
      reaction -> reactionsMatcher.isReactionSubstructureMatch(reaction.getReactionSmiles(), substructureParams)
    );
  }

  @Override
  public SearchIterator<Flattened.Reaction> searchExact(String[] libraryIds, byte[] participantExactFingerprint,
                                                        ExactParams exactParams, ReactionParticipantRole role) {
    var matchQueryBuilder = QueryBuilders.match(m -> m.field(ElasticIndexMappingsFactory.EXACT)
      .query(FingerPrintUtilities.exactHash(participantExactFingerprint)));

    var boolQuery = QueryBuilders.bool()
      .filter(matchQueryBuilder)
      .filter(buildReactionParticipantsRoleQuery(role))
      .build();
    return searchForReactionsByReactionParticipants(
      libraryIds,
      getSearchRequestBuilder(boolQuery._toQuery()),
      reactionParticipantDocument -> reactionsMatcher.isExactMatch(
        FingerPrintUtilities.decodeStructure(reactionParticipantDocument.getStructure()), exactParams)
    );
  }

  @Override
  public SearchIterator<Flattened.Reaction> searchSub(String[] libraryIds, byte[] participantSubstructureFingerprint,
                                                      SubstructureParams substructureParams,
                                                      ReactionParticipantRole role) {
    var query = searchStructureBuilder(participantSubstructureFingerprint)
      .filter(buildReactionParticipantsRoleQuery(role)).build()._toQuery();
    return searchForReactionsByReactionParticipants(
      libraryIds,
      getSearchRequestBuilder(query),
      reactionParticipantDocument -> reactionsMatcher.isSubstructureMatch(
        FingerPrintUtilities.decodeStructure(reactionParticipantDocument.getStructure()), substructureParams)
    );
  }

  @Override
  public ItemWriter<ReactionUploadDocument> itemWriter(String libraryId) {
    return new ElasticsearchReactionUploader(asyncClient, libraryId, fingerPrintCalculator(),
      this::updateLibraryDocument);
  }

  @Override
  public long countElements(String libraryId) {
    var indexName = getReactionIndexName(libraryId);
    return super.countElements(indexName);
  }

  @Override
  public boolean deleteReactions(String libraryId, List<String> reactionIds) {
    return super.deleteItemsAndUpdateLibrarySize(libraryId, reactionIds);
  }

  @Override
  long deleteItems(String libraryId, List<String> reactionIds) {
    // delete reaction participants
    var queryBuilder = QueryBuilders.bool();
    reactionIds
      .forEach(
        reactionId -> queryBuilder.should(
          QueryBuilders.match(m -> m.field(ElasticIndexMappingsFactory.REACTION_ID).query(reactionId))));
    var query = queryBuilder.build()._toQuery();
    deleteByQuery(getReactionParticipantIndexName(libraryId), query);
    // delete reactions
    return super.deleteItems(getReactionIndexName(libraryId), reactionIds);
  }

  List<String> getReactionIndexNames(String[] libraryIds) {
    return Arrays.stream(libraryIds)
      .map(ElasticsearchStorageReactions::getReactionIndexName)
      .toList();
  }

  List<String> getReactionParticipantIndexNames(String[] libraryIds) {
    return Arrays.stream(libraryIds)
      .map(ElasticsearchStorageReactions::getReactionParticipantIndexName)
      .toList();
  }

  private List<Flattened.Reaction> findByIds(String[] libraryIds, String... reactionIds) {
    try {
      var indexNames = getReactionIndexNames(libraryIds);
      var libraries = storageLibrary.getLibraryById(Arrays.asList(libraryIds));
      var queryBuilder = QueryBuilders.ids().values(Arrays.asList(reactionIds));
      var request = new SearchRequest.Builder()
        .query(queryBuilder.build()._toQuery())
        .size(reactionIds.length);
      var searchResponse = searchForReactions(indexNames, request);
      return getReactionsFromSearchHits(searchResponse.hits().hits(), libraries);
    } catch (IOException e) {
      log.error("Failed to search reaction by ids: {} in libraries: {}", reactionIds, libraryIds, e);
      return List.of();
    }
  }

  private Query buildReactionParticipantsRoleQuery(ReactionParticipantRole role) {
    if (role == ReactionParticipantRole.spectator) {
      var solventTerm = QueryBuilders.term(t -> t.field(ElasticIndexMappingsFactory.ROLE)
        .value(ReactionParticipantRole.solvent.toString()));
      var catalystTerm = QueryBuilders.term(t -> t.field(ElasticIndexMappingsFactory.ROLE)
        .value(ReactionParticipantRole.catalyst.toString()));
      var solventQuery = new BoolQuery.Builder().must(solventTerm).mustNot(catalystTerm).build()._toQuery();
      var catalystQuery = new BoolQuery.Builder().must(catalystTerm).mustNot(solventTerm).build()._toQuery();
      return QueryBuilders.bool().should(solventQuery).should(catalystQuery).minimumShouldMatch("1").build()._toQuery();
    } else {
      return QueryBuilders.term(q -> q.field(ElasticIndexMappingsFactory.ROLE)
        .value(role.toString())).term()._toQuery();
    }
  }

  private SearchResponse<Object> searchForReactions(List<String> indexNames, SearchRequest.Builder requestBuilder)
    throws IOException {
    var request = requestBuilder.index(indexNames).build();
    log.trace("Elastic client search method call with parameters {}, {}", request, RequestOptions.DEFAULT);
    var searchResponse = client.search(request, Object.class);
    log.trace("Elastic client search method return result: {}", searchResponse);
    return searchResponse;
  }

  private List<Flattened.ReactionParticipant> getReactionParticipantsFromSearchHits(List<Hit<Object>> hits) {
    return hits.stream()
      .map(this::tryToGetReactionParticipant)
      .flatMap(Optional::stream)
      .toList();
  }

  private SearchIterator<Flattened.Reaction> searchForReactionsByReactionParticipants(String[] libraryIds,
                                                                                      SearchRequest.Builder builder,
                                                                                      Predicate<ReactionParticipantDocument> filter) {
    var indexNames = getReactionParticipantIndexNames(libraryIds);
    var request = builder.index(indexNames).requestCache(false).scroll(timeValue(properties.getScrollTimeout()));

    if (properties.getMaxConcurrentShardRequests() > 0) {
      request.maxConcurrentShardRequests((long) properties.getMaxConcurrentShardRequests());
    }
    return new ElasticsearchIterator.Reactions(
      transport,
      request.build(),
      timeValue(properties.getScrollTimeout()),
      hits -> getReactionsFromReactionParticipantSearchHits(libraryIds, hits, filter),
      List.of(libraryIds)
    );
  }

  private List<Flattened.Reaction> getReactionsFromReactionParticipantSearchHits(String[] libraryIds,
                                                                                 List<Hit<Object>> hits,
                                                                                 Predicate<ReactionParticipantDocument> filter) {
    List<String> reactionIds = hits.stream()
      .map(this::tryToGetReactionParticipantDocument)
      .flatMap(Optional::stream)
      .filter(filter)
      .map(ReactionParticipantDocument::getReactionId)
      .toList();

    if (reactionIds.isEmpty()) {
      return List.of();
    }

    return findByIds(libraryIds, reactionIds.toArray(new String[0]));
  }

  private List<Flattened.Reaction> getReactionsFromSearchHits(List<Hit<Object>> hits, List<Library> libraries) {
    return getReactionsFromSearchHits(hits, libraries, reaction -> true);
  }

  private List<Flattened.Reaction> getReactionsFromSearchHits(List<Hit<Object>> hits, List<Library> libraries,
                                                              Predicate<Flattened.Reaction> filter) {
    var indexMap = libraries.stream().map(Library::getId)
      .collect(Collectors.toMap(ElasticsearchStorageReactions::getReactionIndexName, Function.identity()));
    return hits.stream()
      .map(hit -> tryToGetReaction(hit, indexMap, libraries))
      .flatMap(Optional::stream)
      .filter(filter)
      .toList();
  }

  private Optional<Flattened.Reaction> tryToGetReaction(Hit<Object> hit, Map<String, String> indexMap,
                                                        List<Library> libraries) {
    String json = JsonMapper.objectToJSON(hit.source());
    try {
      var reactionDocument = JsonMapper.toReactionDocument(json);
      var reaction = Mapper.flattenReaction(hit.id(), indexMap.get(hit.index()), reactionDocument, libraries);
      return Optional.of(reaction);
    } catch (IOException e) {
      log.error("Failed to parse json {} to {}", hit, ReactionDocument.class, e);
      return Optional.empty();
    }
  }

  private Optional<Flattened.ReactionParticipant> tryToGetReactionParticipant(Hit<Object> hit) {
    return tryToGetReactionParticipantDocument(hit)
      .map(reactionParticipantDocument ->
        Mapper.flattenReactionParticipant(hit.id(), reactionParticipantDocument));
  }

  private Optional<ReactionParticipantDocument> tryToGetReactionParticipantDocument(Hit<Object> hit) {
    String json = JsonMapper.objectToJSON(hit.source());
    try {
      var reactionParticipantDocument = JsonMapper.toReactionParticipantDocument(json);
      return Optional.of(reactionParticipantDocument);
    } catch (IOException e) {
      log.error("Failed to parse json {} to {}", hit, ReactionParticipantDocument.class, e);
      return Optional.empty();
    }
  }

  private SearchIterator<Flattened.Reaction> searchForReactions(String[] libraryIds, BoolQuery boolQuery,
                                                                Predicate<Flattened.Reaction> filter) {
    var indexNames = getReactionIndexNames(libraryIds);
    var request = getSearchRequestBuilder(boolQuery._toQuery()).index(indexNames).requestCache(false)
      .scroll(timeValue(properties.getScrollTimeout()));
    var libraries = storageLibrary.getLibraryById(Arrays.asList(libraryIds));

    if (properties.getMaxConcurrentShardRequests() > 0) {
      request.maxConcurrentShardRequests((long) properties.getMaxConcurrentShardRequests());
    }
    return new ElasticsearchIterator.Reactions(
      transport,
      request.build(),
      timeValue(properties.getScrollTimeout()),
      hits -> getReactionsFromSearchHits(hits, libraries, filter),
      Arrays.asList(libraryIds)
    );
  }

  private <K, T> List<T> scrollIterator(ElasticsearchIterator<K, T> iterator) {
    List<T> results = new ArrayList<>();
    try (iterator) {
      List<T> next;
      do {
        next = iterator.next();
        results.addAll(next);
      } while (!next.isEmpty());
    }
    return results;
  }
}
