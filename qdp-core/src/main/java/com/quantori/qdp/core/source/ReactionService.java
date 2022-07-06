package com.quantori.qdp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import com.quantori.qdp.core.source.model.DataLibrary;
import com.quantori.qdp.core.source.model.DataLibraryType;
import com.quantori.qdp.core.source.model.DataSource;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.PipelineStatistics;
import com.quantori.qdp.core.source.model.StorageType;
import com.quantori.qdp.core.source.model.TransformationStep;
import com.quantori.qdp.core.source.model.reaction.Reaction;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class ReactionService {
  private final ActorSystem<?> actorSystem;
  private final ActorRef<ReactionSourceRootActor.Command> rootActorRef;

  public ReactionService() {
    this(ActorSystem.create(ReactionSourceRootActor.create(), "qdp-akka-system"));
  }

  public ReactionService(ActorSystem<ReactionSourceRootActor.Command> system) {
    this.actorSystem = system;
    this.rootActorRef = system;
  }

  /**
   * Registers DataStorage instance with given name.
   */
  public void registerReactionStorage(DataStorage<Reaction> storage, String storageName, int maxUploads) {
    //TODO: add timeout.
    createSource(storageName, maxUploads, StorageType.EXTERNAL, storage).toCompletableFuture().join();
  }

  public <S> CompletionStage<PipelineStatistics> loadReactionsFromDataSource(
      String storageName, String libraryName, DataSource<S> dataSource,
      TransformationStep<S, Reaction> transformation) {
    return findSourceActor(storageName)
        .thenCompose(sourceActorDescription ->
            loadFromDataSource(libraryName, dataSource, transformation, sourceActorDescription.actorRef));
  }

  private <S> CompletionStage<PipelineStatistics> loadFromDataSource(
      String libraryName, DataSource<S> dataSource, TransformationStep<S, Reaction> transformation,
      ActorRef<ReactionSourceActor.Command> sourceActorRef) {
    return AskPattern.askWithStatus(
        sourceActorRef,
        replyTo -> new ReactionSourceActor.LoadFromDataSource(libraryName, dataSource, transformation, replyTo),
        //TODO: probably not ideal solution to have long timeout here.
        Duration.ofDays(1),
        actorSystem.scheduler()
    );
  }

  private CompletionStage<ActorRef<ReactionSourceActor.Command>> createSource(
      String storageName, int maxUploads, StorageType storageType, DataStorage<Reaction> storage) {
    return AskPattern.askWithStatus(
        rootActorRef,
        replyTo -> new ReactionSourceRootActor.CreateSource(replyTo, storageName, maxUploads, storageType, storage),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  private CompletionStage<ReactionSourceRootActor.SourceActorDescription> findSourceActor(String storageName) {
    return listSources()
        .thenApply(sourceActorDescriptions -> sourceActorDescriptions.stream()
            .filter(item -> item.storageName.equals(storageName)).findFirst()
            .orElseThrow(() -> new RuntimeException("Cannot find source actor for " + storageName)));
  }

  private CompletionStage<DataLibrary> sendMessageFindLibrary(ActorRef<ReactionSourceActor.Command> actorRef,
                                                              String libraryName, DataLibraryType libraryType) {
    return AskPattern.askWithStatus(
        actorRef,
        replyTo -> new ReactionSourceActor.FindLibrary(replyTo, libraryName, libraryType),
        Duration.ofMinutes(1),
        actorSystem.scheduler()
    );
  }

  public CompletionStage<List<ReactionSourceRootActor.SourceActorDescription>> listSources() {
    return AskPattern.ask(
        rootActorRef,
        ReactionSourceRootActor.GetSources::new,
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }


  public CompletionStage<DataLibrary> findLibrary(final String storageName, final String libraryName,
                                                  final DataLibraryType libraryType) {
    return findSourceActor(storageName).thenCompose(d -> sendMessageFindLibrary(d.actorRef, libraryName, libraryType));
  }
}
