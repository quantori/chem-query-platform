package com.quantori.cqp.storage.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import com.quantori.cqp.api.ItemWriter;
import com.quantori.cqp.api.MoleculesFingerprintCalculator;
import com.quantori.cqp.api.model.upload.Molecule;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

@Slf4j
class ElasticsearchMoleculeUploader extends BulkProcessorItemWriter implements ItemWriter<Molecule> {

  private final String libraryId;
  private final MoleculesFingerprintCalculator fingerprintCalculator;

  ElasticsearchMoleculeUploader(ElasticsearchAsyncClient client, String libraryId,
                                MoleculesFingerprintCalculator fingerprintCalculator,
                                Consumer<String> updateLibraryDocument) {
    super(client, () -> updateLibraryDocument.accept(libraryId));
    this.libraryId = libraryId;
    this.fingerprintCalculator = fingerprintCalculator;
  }

  @Override
  public void write(Molecule molecule) {
    try {
      var moleculeDocument = Mapper.convertToDocument(molecule, fingerprintCalculator);
      var json = JsonMapper.toJsonString(moleculeDocument);

      InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(json.getBytes()),
          StandardCharsets.UTF_8);

      BulkOperation operation = new BulkOperation.Builder()
          .index(op -> op
              .index(libraryId)
              .id(molecule.getId())
              .document(readJson(reader))
          ).build();

      write(operation);
    } catch (IOException e) {
      log.warn("Failed to add molecule to storage", e);
    }
  }
}
