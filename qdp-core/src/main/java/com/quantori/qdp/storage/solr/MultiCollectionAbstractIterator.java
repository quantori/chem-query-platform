package com.quantori.qdp.storage.solr;

import com.quantori.qdp.storage.api.Library;
import com.quantori.qdp.storage.api.SearchIterator;
import java.util.ArrayDeque;
import java.util.List;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.common.SolrDocumentList;

abstract class MultiCollectionAbstractIterator<T> implements SearchIterator<T> {
  private final SolrClient client;
  private final SolrQuery solrQuery;
  private final ArrayDeque<Library> libraries;

  protected MultiCollectionAbstractIterator(SolrClient client, SolrQuery solrQuery, List<Library> libraries) {
    this.client = client;
    this.solrQuery = solrQuery;
    this.libraries = new ArrayDeque<>(libraries);
  }

  protected String getCollectionName(Library library) {
    return library.getId();
  }

  protected abstract List<T> getSearchResult(final SolrDocumentList results, final Library library);

  @Override
  public List<T> next() {
    if (libraries.isEmpty()) {
      return List.of();
    }
    var results = search();
    if (results.isEmpty()) {
      return List.of();
    }
    return getSearchResult(results, libraries.peek());
  }

  private SolrDocumentList search() {
    if (!libraries.isEmpty()) {
      var results = tryToScroll(getCollectionName(libraries.peek()));
      if (!results.isEmpty()) {
        solrQuery.setStart(solrQuery.getStart() + solrQuery.getRows());
        return results;
      } else {
        solrQuery.setStart(0);
        libraries.remove();
        return search();
      }
    }
    return new SolrDocumentList();
  }

  private SolrDocumentList tryToScroll(String collectionName) {
    try {
      return client.query(collectionName, solrQuery).getResults();
    } catch (Exception e) {
      throw new RuntimeException(String.format("Bad SOLR scroll request %s for collectionName %s",
          solrQuery, collectionName), e);
    }
  }
}
