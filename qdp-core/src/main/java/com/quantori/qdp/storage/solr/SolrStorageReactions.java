package com.quantori.qdp.storage.solr;

import com.quantori.qdp.storage.api.FingerPrintUtilities;
import com.quantori.qdp.storage.api.Flattened;
import com.quantori.qdp.storage.api.Library;
import com.quantori.qdp.storage.api.ReactionsWriter;
import com.quantori.qdp.storage.api.SearchIterator;
import com.quantori.qdp.storage.api.StorageReactions;
import com.quantori.qdp.storage.api.StorageType;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

@Slf4j
@RequiredArgsConstructor
public class SolrStorageReactions implements StorageReactions {
  public static final int DEFAULT_SEARCH_PAGE_SIZE = 100;
  public static final String REACTION_PARTICIPANTS_STORE_PREFIX = "reactions_participant_";
  public static final String REACTIONS_STORE_PREFIX = "reactions_";

  private static final String DEFAULT_QUERY_STRING = "*:*";
  private final boolean rollbackEnabled;
  private final HttpSolrClient solrClient;
  private final ConcurrentUpdateSolrClient solrUpdateClient;
  private final SolrStorageLibrary storageLibrary;

  @Override
  public Optional<Flattened.Reaction> searchById(Library library, String reactionId) {
    var solrQuery = new SolrQuery(DEFAULT_QUERY_STRING).setFilterQueries("id:" + reactionId);
    try {
      var query = solrClient.query(REACTIONS_STORE_PREFIX + library.getId(), solrQuery);
      return query.getResults().stream()
          .map(document -> Mapper.flattenReaction(document, library.getId()))
          .reduce((one, two) -> {
            throw new SolrStorageException("Multiple reactions found by unique id");
          });
    } catch (SolrServerException | IOException e) {
      log.warn("Failed to find document by {} in library {}", reactionId, library, e);
    }
    return Optional.empty();
  }

  @Override
  public List<Flattened.ReactionParticipant> searchParticipantsByReactionId(Library library, String reactionId) {
    var solrQuery = new SolrQuery(DEFAULT_QUERY_STRING).setFilterQueries("reactionId:" + reactionId);
    var collectionName = REACTION_PARTICIPANTS_STORE_PREFIX + library.getId();
    try {
      final var result = solrClient.query(collectionName, solrQuery);
      return result.getResults().stream()
          .map(Mapper::flattenReactionParticipant)
          .toList();
    } catch (Exception e) {
      log.warn("Reaction participant search by id {} in collection {} failed.", reactionId, library, e);
      return List.of();
    }
  }

  @Override
  public SearchIterator<Flattened.Reaction> searchReactions(
      String[] libraryIds, byte[] reactionSubstructureFingerprint) {
    var solrQuery = new SolrQuery(DEFAULT_QUERY_STRING)
        .setRows(DEFAULT_SEARCH_PAGE_SIZE)
        .setStart(0)
        .setSort("id", SolrQuery.ORDER.asc);

    if (FingerPrintUtilities.isNotBlank(reactionSubstructureFingerprint)) {
      var subQuery = "(" + FingerPrintUtilities.substructureHash(reactionSubstructureFingerprint) + ")";
      solrQuery.setQuery(new TermQuery(new Term("sub", subQuery)).toString());
      solrQuery.add("q.op", "AND");
    }

    var libraries = Arrays.stream(libraryIds)
        .map(storageLibrary::getLibraryById)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .toList();
    return new SolrReactionsSearchIterator(solrClient, solrQuery, libraries);
  }

  @Override
  public ReactionsWriter buildReactionWriter(Library library) {
    Consumer<Library> onCloseCallback = libraryElement ->
        storageLibrary.updateLibrarySize(library.getId(), countElements(library.getId()));
    return new SolrReactionsWriter(solrUpdateClient, library, onCloseCallback, rollbackEnabled);
  }

  @Override
  public long countElements(String libraryId) {
    var indexName = REACTIONS_STORE_PREFIX + libraryId;
    var query = new SolrQuery("*:*").setRows(0);
    try {
      var response = solrClient.query(indexName, query);
      return response.getResults().getNumFound();
    } catch (SolrServerException | IOException e) {
      log.warn("Failed to count elements in library {} of storage {}", libraryId, StorageType.solr, e);
      throw new SolrStorageException("Failed to count elements", e);
    }
  }

  @Override
  public String toString() {
    return "StorageReactions{" + StorageType.solr + '}';
  }
}
