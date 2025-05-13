package com.quantori.cqp.storage.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import com.quantori.cqp.api.model.upload.Reaction;
import com.quantori.cqp.api.model.upload.ReactionParticipant;
import com.quantori.cqp.api.model.upload.ReactionUploadDocument;
import com.quantori.cqp.api.ReactionsFingerprintCalculator;
import com.quantori.cqp.core.model.ItemWriter;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Consumer;

@Slf4j
class ElasticsearchReactionUploader extends BulkProcessorItemWriter implements ItemWriter<ReactionUploadDocument> {
  private final String reactionIndexName;
  private final String reactionParticipantIndexName;
  private final ReactionsFingerprintCalculator fingerprintCalculator;


  ElasticsearchReactionUploader(ElasticsearchAsyncClient client, String libraryId,
                                ReactionsFingerprintCalculator fingerprintCalculator,
                                Consumer<String> updateLibraryDocument) {
    super(client, () -> updateLibraryDocument.accept(libraryId));
    this.reactionIndexName = ElasticsearchStorageReactions.getReactionIndexName(libraryId);
    this.reactionParticipantIndexName = ElasticsearchStorageReactions.getReactionParticipantIndexName(libraryId);
    this.fingerprintCalculator = fingerprintCalculator;
    log.debug("Reaction uploader initialized to added reactions to index {} and participant to index {}",
        reactionIndexName, reactionParticipantIndexName);
  }

  @Override
  public void write(ReactionUploadDocument reactionUploadDocument) {
    try {
      var reactionId = saveReaction(reactionUploadDocument.reaction());
      var amount = saveParticipants(reactionId, reactionUploadDocument.participantEntities());
      log.debug("total participant added to storage {}, of reaction {}", amount, reactionId);
    } catch (IOException e) {
      log.warn("Failed to add reaction to storage", e);
    }
  }

  private String saveReaction(Reaction reaction) throws IOException {
    var reactionId = Objects.requireNonNullElse(reaction.getId(), UUID.randomUUID().toString());
    var reactionDocument = Mapper.convertToDocument(reaction, fingerprintCalculator);
    var json = JsonMapper.toJsonString(reactionDocument);

    InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(json.getBytes()),
        StandardCharsets.UTF_8);

    BulkOperation operation = new BulkOperation.Builder()
        .create(op -> op
            .index(reactionIndexName)
            .id(reactionId)
            .document(readJson(reader))
        ).build();

    write(operation);
    return reactionId;
  }

  private int saveParticipants(String reactionId, List<ReactionParticipant> reactionParticipants) {
    if (reactionParticipants == null) {
      return 0;
    }
    int counter = 0;
    for (ReactionParticipant reactionParticipant : reactionParticipants) {
      reactionParticipant.setReactionId(reactionId);
      try {
        var reactionParticipantDocument = Mapper.convertToDocument(reactionParticipant, fingerprintCalculator);
        var json = JsonMapper.toJsonString(reactionParticipantDocument);

        InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(json.getBytes()),
            StandardCharsets.UTF_8);

        BulkOperation operation = new BulkOperation.Builder()
            .create(op -> op
                .index(reactionParticipantIndexName)
                .id(reactionParticipant.getId())
                .document(readJson(reader))
            ).build();

        write(operation);
        counter++;
      } catch (IOException e) {
        log.warn("Failed to convert reaction reactionParticipant to json", e);
      }
    }
    return counter;
  }
}
