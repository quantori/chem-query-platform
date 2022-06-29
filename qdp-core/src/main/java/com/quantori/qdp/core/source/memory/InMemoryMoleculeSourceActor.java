package com.quantori.qdp.core.source.memory;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.completedStage;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import com.quantori.qdp.core.source.MoleculeLoader;
import com.quantori.qdp.core.source.MoleculeSearchActor;
import com.quantori.qdp.core.source.MoleculeSourceActor;
import com.quantori.qdp.core.source.model.DataLibrary;
import com.quantori.qdp.core.source.model.DataLibraryType;
import com.quantori.qdp.core.source.model.DataSource;
import com.quantori.qdp.core.source.model.PipelineStatistics;
import com.quantori.qdp.core.source.model.molecule.Molecule;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryMoleculeSourceActor extends MoleculeSourceActor {
  private final Logger logger = LoggerFactory.getLogger(getClass());

  private final InMemoryLibraryStorage storage = new InMemoryLibraryStorage();

  private InMemoryMoleculeSourceActor(ActorContext<Command> context, String storageName, int maxUploads) {
    super(context, storageName, maxUploads);
  }

  public static Behavior<Command> create(String storageName, int maxUploads) {
    return Behaviors.setup(ctx -> new InMemoryMoleculeSourceActor(ctx, storageName, maxUploads));
  }

  @Override
  protected CompletionStage<List<DataLibrary>> getLibraries() {
    List<DataLibrary> libraries = storage.keySet().stream()
        .map(qdpMolecules -> new DataLibrary(qdpMolecules, DataLibraryType.MOLECULE, emptyMap()))
        .collect(Collectors.toList());
    return completedStage(libraries);
  }

  @Override
  protected CompletionStage<DataLibrary> createLibrary(CreateLibrary cmd) {
    if (storage.containsKey(cmd.dataLibrary.getName())) {
      //TODO: replace RuntimeException by something cool
      return CompletableFuture.failedFuture(new RuntimeException("Library " + cmd.dataLibrary + " already exist"));
    }
    storage.put(cmd.dataLibrary.getName(), new ArrayList<>());
    return completedStage(new DataLibrary(cmd.dataLibrary));
  }

  @Override
  protected CompletionStage<DataLibrary> findLibrary(final FindLibrary cmd) {
    return CompletableFuture.supplyAsync(() -> storage.entrySet()
        .stream()
        .filter(x -> cmd.libraryName.equals(x.getKey()))
        .findFirst()
        .map(entry -> DataLibrary.builder().name(cmd.libraryName).type(cmd.libraryType).build())
        .orElseThrow());
  }

  @Override
  protected <S> CompletionStage<PipelineStatistics> loadFromDataSource(LoadFromDataSource<S> command) {
    final List<Molecule> molecules = storage.get(command.libraryId);

    if (molecules == null) {
      //TODO: replace RuntimeException by something cool
      return CompletableFuture.failedFuture(new RuntimeException("Library " + command.libraryId + " not found"));
    }

    return completedStage(command).thenComposeAsync(cmd -> {
      final DataSource<S> dataSource = cmd.dataSource;

      final var loader = new MoleculeLoader(getContext().getSystem());
      return loader.loadMolecules(dataSource, cmd.transformation, molecules::add)
          .whenComplete((done, error) -> close(dataSource, logger));
    });

  }

  @Override
  protected ActorRef<MoleculeSearchActor.Command> createSearchActor(CreateSearch createSearchCmd, String searchId) {
    ActorRef<MoleculeSearchActor.Command> searchRef = getContext().spawn(
        InMemorySearchActor.create(searchId, storage), "search-" + searchId);
    getContext().getLog().info("Created search actor: {}", searchRef);
    return searchRef;
  }
}
