package com.quantori.qdp.storage.solr;

import com.quantori.qdp.storage.api.Flattened;
import com.quantori.qdp.storage.api.Library;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocumentList;

@Slf4j
class SolrReactionsSearchIterator extends MultiCollectionAbstractIterator<Flattened.Reaction> {

  SolrReactionsSearchIterator(SolrClient client, SolrQuery solrQuery, List<Library> libraries) {
    super(client, solrQuery, libraries);
  }

  @Override
  protected String getCollectionName(Library library) {
    return SolrStorageReactions.REACTIONS_STORE_PREFIX + library.getId();
  }

  @Override
  protected List<Flattened.Reaction> getSearchResult(final SolrDocumentList results, final Library library) {
    return results.stream()
        .map(document -> Mapper.flattenReaction(document, library.getId()))
        .toList();
  }
}
