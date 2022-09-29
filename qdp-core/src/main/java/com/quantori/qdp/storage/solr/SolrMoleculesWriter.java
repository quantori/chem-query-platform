package com.quantori.qdp.storage.solr;

import com.quantori.qdp.storage.api.Molecule;
import com.quantori.qdp.storage.api.MoleculesWriter;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;

@Slf4j
public class SolrMoleculesWriter implements MoleculesWriter {
  private static final int COMMIT_WITHIN_MS = 1000;
  private final SolrClient client;
  private final Runnable callback;
  private final boolean rollbackEnabled;
  private final String libraryId;

  SolrMoleculesWriter(SolrClient client, String libraryId, Runnable callback, boolean rollbackEnabled) {
    this.client = client;
    this.callback = callback;
    this.rollbackEnabled = rollbackEnabled;
    this.libraryId = libraryId;
  }

  @Override
  public void write(Molecule molecule) {
    try {
      var updateResponse = client.addBean(libraryId, SolrMoleculeDocument
          .withGeneratedId(molecule, libraryId), COMMIT_WITHIN_MS);
      if (updateResponse.getStatus() != 0) {
        throw new RuntimeException(
            String.format("SOLR Error while adding document: %s into collection: %s. Error status: %d", molecule,
                libraryId, updateResponse.getStatus()));
      }
      log.trace("Successfully added molecule document: {}, into collection: {}", molecule, libraryId);
    } catch (Exception e) {
      log.warn("Error adding molecule document: {} into collection: {}.", molecule, libraryId, e);
    }
  }

  @Override
  public void flush() {
    try {
      client.commit(libraryId);
      log.trace("Successfully flushed molecules bulk upload into collection: {}", libraryId);
    } catch (SolrServerException | IOException e) {
      log.warn("Error flushing into collection: {}. Error message: {}", libraryId, e);
      if (rollbackEnabled) {
        try {
          var rollback = client.rollback(libraryId);
          log.trace("Solr rollback response: {}", rollback);
        } catch (SolrServerException | IOException ee) {
          log.error("Error while rollback", ee);
        }
      }
    }
  }

  @Override
  public void close() {
    try {
      flush();
    } finally {
      try {
        callback.run();
      } finally {
        flush();
      }
    }
  }
}
