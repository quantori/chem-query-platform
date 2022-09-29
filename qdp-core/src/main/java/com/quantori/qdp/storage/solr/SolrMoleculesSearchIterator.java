package com.quantori.qdp.storage.solr;

import static com.quantori.qdp.storage.solr.SolrStorageReactions.REACTION_PARTICIPANTS_STORE_PREFIX;

import com.quantori.qdp.storage.api.Flattened;
import com.quantori.qdp.storage.api.Library;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocumentList;

@Slf4j
class SolrMoleculesSearchIterator extends MultiCollectionAbstractIterator<Flattened.Molecule> {
  private final boolean isParticipantSearch;

  SolrMoleculesSearchIterator(SolrClient client, SolrQuery solrQuery, boolean isParticipantsSearch,
                              List<Library> libraries) {
    super(client, solrQuery, libraries);
    this.isParticipantSearch = isParticipantsSearch;
  }

  @Override
  protected List<Flattened.Molecule> getSearchResult(SolrDocumentList results, Library library) {
    return SolrStorageMolecules.getSearchResultSetSolr(results, isParticipantSearch, library.getId(),
        library.getName());
  }

  @Override
  protected String getCollectionName(Library library) {
    return (isParticipantSearch ? REACTION_PARTICIPANTS_STORE_PREFIX : "") + library.getId();
  }
}
