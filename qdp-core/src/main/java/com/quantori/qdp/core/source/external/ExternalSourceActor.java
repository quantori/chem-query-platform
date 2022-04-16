package com.quantori.qdp.core.source.external;

import static java.util.concurrent.CompletableFuture.completedStage;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import com.quantori.qdp.core.source.MoleculeLoader;
import com.quantori.qdp.core.source.MoleculeSearchActor;
import com.quantori.qdp.core.source.MoleculeSourceActor;
import com.quantori.qdp.core.source.model.DataLibrary;
import com.quantori.qdp.core.source.model.DataLoader;
import com.quantori.qdp.core.source.model.DataSource;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.PipelineStatistics;
import com.quantori.qdp.core.source.model.molecule.Molecule;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExternalSourceActor extends MoleculeSourceActor {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final DataStorage<Molecule> storage;

  private ExternalSourceActor(ActorContext<Command> context, String storageName, int maxUploads,
                              DataStorage<Molecule> storage) {
    super(context, storageName, maxUploads);
    this.storage = storage;
  }

  public static Behavior<Command> create(String storageName, int maxUploads, DataStorage<Molecule> storage) {
    return Behaviors.setup(ctx -> new ExternalSourceActor(ctx, storageName, maxUploads, storage));
  }

  @Override
  protected CompletionStage<List<DataLibrary>> getLibraries() {
    return CompletableFuture.supplyAsync(storage::getLibraries);
  }

  @Override
  protected CompletionStage<DataLibrary> createLibrary(CreateLibrary cmd) {
    return CompletableFuture.supplyAsync(() -> storage.createLibrary(cmd.dataLibrary));
  }

  protected CompletionStage<DataLibrary> findLibrary(FindLibrary cmd) {
    return CompletableFuture.supplyAsync(() -> storage.findLibrary(cmd.libraryName, cmd.libraryType));
  }

  @Override
  protected <S> CompletionStage<PipelineStatistics> loadFromDataSource(LoadFromDataSource<S> command) {
    return completedStage(command).thenComposeAsync(cmd -> {
      final DataLoader<Molecule> storageLoader = storage.dataLoader(cmd.libraryId);
      final DataSource<S> dataSource = cmd.dataSource;

      final var loader = new MoleculeLoader(getContext().getSystem());
      return loader.loadMolecules(dataSource, cmd.transformation, storageLoader::add)
          .whenComplete((done, error) -> close(dataSource, logger))
          .whenComplete((done, error) -> close(storageLoader, logger));
    });
  }

  @Override
  protected ActorRef<MoleculeSearchActor.Command> createSearchActor(CreateSearch createSearchCmd) {
    ActorRef<MoleculeSearchActor.Command> searchRef = getContext().spawn(
        ExternalSearchActor.create(storageName, storage), "search-" + UUID.randomUUID());
    getContext().getLog().info("Created search actor: {}", searchRef);
    return searchRef;
  }
}
