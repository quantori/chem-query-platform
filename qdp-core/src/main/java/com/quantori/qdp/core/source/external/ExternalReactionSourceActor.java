package com.quantori.qdp.core.source.external;

import static java.util.concurrent.CompletableFuture.completedStage;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import com.quantori.qdp.core.source.ReactionLoader;
import com.quantori.qdp.core.source.ReactionSourceActor;
import com.quantori.qdp.core.source.model.DataLibrary;
import com.quantori.qdp.core.source.model.DataLoader;
import com.quantori.qdp.core.source.model.DataSource;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.PipelineStatistics;
import com.quantori.qdp.core.source.model.reaction.Reaction;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalReactionSourceActor extends ReactionSourceActor {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final DataStorage<Reaction> storage;

  private ExternalReactionSourceActor(ActorContext<Command> context, String storageName, int maxUploads,
                              DataStorage<Reaction> storage) {
    super(context, storageName, maxUploads);
    this.storage = storage;
  }

  public static Behavior<Command> create(String storageName, int maxUploads, DataStorage<Reaction> storage) {
    return Behaviors.setup(ctx -> new ExternalReactionSourceActor(ctx, storageName, maxUploads, storage));
  }
  @Override
  protected CompletionStage<DataLibrary> findLibrary(FindLibrary cmd) {
    return CompletableFuture.supplyAsync(() -> storage.findLibrary(cmd.libraryName, cmd.libraryType));
  }

  @Override
  protected <S> CompletionStage<PipelineStatistics> loadFromDataSource(ReactionSourceActor.LoadFromDataSource<S> command) {
    return completedStage(command).thenComposeAsync(cmd -> {
      final DataLoader<Reaction> storageLoader = storage.dataLoader(cmd.libraryId);
      final DataSource<S> dataSource = cmd.dataSource;

      final var loader = new ReactionLoader(getContext().getSystem());
      return loader.loadReactions(dataSource, cmd.transformation, storageLoader::add)
          .whenComplete((done, error) -> close(dataSource, logger))
          .whenComplete((done, error) -> close(storageLoader, logger));
    });
  }

  @Override
  protected CompletionStage<List<DataLibrary>> getLibraries() {
    return CompletableFuture.supplyAsync(storage::getLibraries);
  }
}
