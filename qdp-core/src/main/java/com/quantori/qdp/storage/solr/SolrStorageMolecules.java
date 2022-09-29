package com.quantori.qdp.storage.solr;

import com.quantori.qdp.storage.api.FingerPrintUtilities;
import com.quantori.qdp.storage.api.Flattened;
import com.quantori.qdp.storage.api.Library;
import com.quantori.qdp.storage.api.MoleculesWriter;
import com.quantori.qdp.storage.api.ReactionParticipantRole;
import com.quantori.qdp.storage.api.SearchIterator;
import com.quantori.qdp.storage.api.SearchProperty;
import com.quantori.qdp.storage.api.Similarity;
import com.quantori.qdp.storage.api.StorageMolecules;
import com.quantori.qdp.storage.api.StorageType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrDocumentList;

@Slf4j
@RequiredArgsConstructor
public class SolrStorageMolecules implements StorageMolecules {
  private static final String DEFAULT_QUERY_STRING = "*:*";

  private final boolean rollbackEnabled;
  private final HttpSolrClient solrClient;
  private final ConcurrentUpdateSolrClient solrUpdateClient;
  private final SolrStorageLibrary storageLibrary;

  static List<Flattened.Molecule> getSearchResultSetSolr(
      SolrDocumentList results, boolean isParticipantsSearch, String libraryId, String libraryName) {
    return results.stream()
        .map(document -> isParticipantsSearch
            ? Mapper.flattenReactionParticipant(document, libraryId, libraryName)
            : Mapper.flattenMolecule(document, libraryId, libraryName))
        .toList();
  }

  private List<Library> toLibraries(String[] ids) {
    return Arrays.stream(ids)
        .map(storageLibrary::getLibraryById)
        .flatMap(Optional::stream)
        .toList();
  }

  @Override
  public SearchIterator<Flattened.Molecule> searchSim(String[] libraryIds, byte[] moleculeSimilarityFingerprint,
                                                      List<SearchProperty> properties, Similarity similarity) {
    var solrQuery = new SolrQuery(DEFAULT_QUERY_STRING)
        .setRows(SolrSimilarityScriptBuilder.DEFAULT_SEARCH_PAGE_SIZE)
        .setStart(0);

    var script = new SolrSimilarityScriptBuilder(moleculeSimilarityFingerprint,
        similarity.getMetric(), similarity.getMinSim(), similarity.getMaxSim(), similarity.getAlpha(),
        similarity.getBeta()).build();

    solrQuery.addFilterQuery(script);

    if (!properties.isEmpty()) {
      solrQuery.addFilterQuery(CriteriaBuilder.buildQueryString(properties));
    }

    return new SolrMoleculesSearchIterator(solrClient, solrQuery, false, toLibraries(libraryIds));
  }

  @Override
  public SearchIterator<Flattened.Molecule> searchSub(
      String[] libraryIds, byte[] moleculeSubstructureFingerprint, List<SearchProperty> properties,
      ReactionParticipantRole reactionParticipantRole) {
    var solrQuery = new SolrQuery(DEFAULT_QUERY_STRING)
        .setRows(SolrSimilarityScriptBuilder.DEFAULT_SEARCH_PAGE_SIZE)
        .setStart(0)
        .setSort("id", SolrQuery.ORDER.asc);

    var booleanQuery = new BooleanQuery.Builder();
    boolean isParticipantsSearch = modifySearchQueryForReactionParticipantsSearch(booleanQuery,
        reactionParticipantRole);

    if (isParticipantsSearch) {
      solrQuery.addFilterQuery(booleanQuery.build().toString());
    }

    if (FingerPrintUtilities.isNotBlank(moleculeSubstructureFingerprint)) {
      var subQuery = "(" + FingerPrintUtilities.substructureHash(moleculeSubstructureFingerprint) + ")";
      solrQuery.setQuery(new TermQuery(new Term("sub", subQuery)).toString());
      solrQuery.add("q.op", "AND");
    }

    if (!properties.isEmpty()) {
      solrQuery.addFilterQuery(CriteriaBuilder.buildQueryString(properties));
    }

    var libraries = toLibraries(libraryIds);
    boolean isReactionParticipantSearch = reactionParticipantRole != ReactionParticipantRole.none;
    return new SolrMoleculesSearchIterator(solrClient, solrQuery, isReactionParticipantSearch, libraries);
  }

  @Override
  public SearchIterator<Flattened.Molecule> searchExact(
      String[] libraryIds, byte[] moleculeExactFingerprint, List<SearchProperty> properties,
      ReactionParticipantRole reactionParticipantRole) {

    var solrQuery = new SolrQuery(DEFAULT_QUERY_STRING)
        .setRows(SolrSimilarityScriptBuilder.DEFAULT_SEARCH_PAGE_SIZE)
        .setStart(0)
        .setSort("id", SolrQuery.ORDER.asc);

    var booleanQuery = new BooleanQuery.Builder();
    boolean isParticipantsSearch =
        modifySearchQueryForReactionParticipantsSearch(booleanQuery, reactionParticipantRole);
    if (isParticipantsSearch) {
      solrQuery = solrQuery.addFilterQuery(booleanQuery.build().toString());
    }

    var builder = new BooleanQuery.Builder();
    builder.add(new BooleanClause(new TermQuery(new Term("exact",
        FingerPrintUtilities.exactHash(moleculeExactFingerprint))),
        BooleanClause.Occur.MUST));
    solrQuery = solrQuery.addFilterQuery(builder.build().toString());

    if (!properties.isEmpty()) {
      solrQuery = solrQuery.addFilterQuery(CriteriaBuilder.buildQueryString(properties));
    }

    var libraries = toLibraries(libraryIds);
    return new SolrMoleculesSearchIterator(solrClient, solrQuery, isParticipantsSearch, libraries);
  }

  @Override
  public List<Flattened.Molecule> findById(String[] libraryIds, String... moleculeIds) {
    var molecules = new ArrayList<Flattened.Molecule>();
    var libraries = toLibraries(libraryIds);
    try {
      for (Library library : libraries) {
        var solrDocumentList = solrClient.getById(library.getId(), List.of(moleculeIds));
        if (solrDocumentList != null) {
          var response = solrDocumentList.stream()
              .map(document -> Mapper.flattenMolecule(document, library.getId(), library.getName()))
              .toList();
          molecules.addAll(response);
        }
      }
      Map<String, Flattened.Molecule> moleculeMap = molecules.stream()
          .collect(Collectors.toMap(Flattened.Molecule::getId, Function.identity()));
      return Arrays.stream(moleculeIds)
          .map(moleculeMap::get)
          .filter(Objects::nonNull)
          .toList();
    } catch (IllegalStateException e) {
      log.warn("Duplicate molecules found");
      throw new SolrStorageException("Duplicate molecules found", e);
    } catch (Exception e) {
      log.warn("Error retrieving molecules", e);
      return List.of();
    }
  }

  @Override
  public MoleculesWriter buildMoleculeWriter(final Library library) {
    Runnable onClose = () -> storageLibrary.updateLibrarySize(library.getId(), countElements(library));
    return new SolrMoleculesWriter(solrUpdateClient, library.getId(), onClose, rollbackEnabled);
  }

  @Override
  public long countElements(final Library library) {
    final var indexName = library.getId();
    var query = new SolrQuery("*:*").setRows(0);
    try {
      var response = solrClient.query(indexName, query);
      return response.getResults().getNumFound();
    } catch (SolrServerException | IOException e) {
      log.warn("Failed to count elements in library {} of storage {}", library, StorageType.solr, e);
      throw new SolrStorageException("Failed to count elements", e);
    }
  }

  private boolean modifySearchQueryForReactionParticipantsSearch(
      BooleanQuery.Builder booleanQuery, ReactionParticipantRole reactionParticipantRole) {
    if (reactionParticipantRole == ReactionParticipantRole.none) {
      return false;
    }
    var b1 = new BooleanQuery.Builder();
    if (reactionParticipantRole == ReactionParticipantRole.spectator) {
      b1.add(new BooleanClause(new TermQuery(
              new Term("role", ReactionParticipantRole.solvent.toString())), BooleanClause.Occur.SHOULD))
          .add(new BooleanClause(new TermQuery(
              new Term("role", ReactionParticipantRole.catalyst.toString())), BooleanClause.Occur.SHOULD));
      booleanQuery.add(new BooleanClause(b1.build(), BooleanClause.Occur.MUST));
      return true;
    } else {
      b1.add(new BooleanClause(new TermQuery(new Term("role", reactionParticipantRole.toString())),
          BooleanClause.Occur.MUST));
      booleanQuery.add(new BooleanClause(b1.build(), BooleanClause.Occur.MUST));
      return true;
    }
  }

  @Override
  public String toString() {
    return "StorageMolecules{" + StorageType.solr + "}";
  }
}
