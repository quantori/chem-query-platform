package com.quantori.qdp.core.source;

import static com.quantori.qdp.core.source.MoleculeSearchActor.searchActorKey;
import static com.quantori.qdp.core.source.MoleculeSourceRootActor.rootActorsKey;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import com.quantori.qdp.core.source.model.DataLibrary;
import com.quantori.qdp.core.source.model.DataLibraryType;
import com.quantori.qdp.core.source.model.DataSource;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.PipelineStatistics;
import com.quantori.qdp.core.source.model.StorageType;
import com.quantori.qdp.core.source.model.TransformationStep;
import com.quantori.qdp.core.source.model.molecule.Molecule;
import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import com.quantori.qdp.core.source.model.molecule.search.SearchResult;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MoleculeService {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final int RETRY_COUNT = 600;
  public static final int TIMEOUT_MILLIS = 100;
  public static final int MAX_SEARCH_ACTORS = 100;
  private final ActorSystem<?> actorSystem;
  private final ActorRef<MoleculeSourceRootActor.Command> rootActorRef;

  public MoleculeService() {
    this(ActorSystem.create(MoleculeSourceRootActor.create(MAX_SEARCH_ACTORS), "qdp-akka-system"));
  }

  public MoleculeService(ActorSystem<MoleculeSourceRootActor.Command> system) {
    this.actorSystem = system;
    this.rootActorRef = system;
  }

  public void registerMoleculeStorage(DataStorage<Molecule> storage, String storageName) {
    registerMoleculeStorage(storage, storageName, Integer.MAX_VALUE);
  }

  /**
   * Registers DataStorage instance with given name.
   */
  public void registerMoleculeStorage(DataStorage<Molecule> storage, String storageName, int maxUploads) {
    //TODO: add timeout.
    createSource(storageName, maxUploads, StorageType.EXTERNAL, storage).toCompletableFuture().join();
  }

  /**
   * This is responsibility of client to ensure that data source generated object of same type as
   * molecule transformation step expected.
   */
  public <S> CompletionStage<PipelineStatistics> loadMoleculesFromDataSource(
      String storageName, String libraryName, DataSource<S> dataSource,
      TransformationStep<S, Molecule> transformation) {
    return findSourceActor(storageName)
        .thenCompose(sourceActorDescription ->
            loadFromDataSource(libraryName, dataSource, transformation, sourceActorDescription.actorRef));
  }

  public CompletionStage<List<MoleculeSourceRootActor.SourceActorDescription>> listSources() {
    return AskPattern.ask(
        rootActorRef,
        MoleculeSourceRootActor.GetSources::new,
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  public CompletionStage<SearchRequest> getSearchRequestDescription(String searchId, String user) {
    ServiceKey<MoleculeSearchActor.Command> serviceKey = searchActorKey(searchId);

    CompletionStage<Receptionist.Listing> cf = AskPattern.ask(
        actorSystem.receptionist(),
        ref -> Receptionist.find(serviceKey, ref),
        Duration.ofMinutes(1),
        actorSystem.scheduler());

    return cf.toCompletableFuture().thenCompose(listing -> {
      if (listing.getServiceInstances(serviceKey).size() != 1) {
        return CompletableFuture.failedFuture(new RuntimeException("Search not found: " + searchId));
      }
      var searchActorRef = listing.getServiceInstances(serviceKey).iterator().next();
      return AskPattern.askWithStatus(
          searchActorRef,
          ref -> new MoleculeSearchActor.GetSearchRequest(ref, user),
          Duration.ofMinutes(1),
          actorSystem.scheduler());
    });
  }


  public CompletionStage<List<DataLibrary>> getDataStorageIndexes(String storageName) {
    return findSourceActor(storageName).thenCompose(d -> getIndexes(d.actorRef));
  }

  public CompletionStage<DataLibrary> findLibrary(final String storageName, final String libraryName,
                                                  final DataLibraryType libraryType) {
    return findSourceActor(storageName).thenCompose(d -> sendMessageFindLibrary(d.actorRef, libraryName, libraryType));
  }

  public CompletionStage<DataLibrary> createDataStorageIndex(String storageName, DataLibrary index) {
    return findSourceActor(storageName).thenCompose(d -> createIndex(d.actorRef, index));
  }

  public CompletionStage<SearchResult> search(SearchRequest request) {
    validate(request);

    return findSourceActor(request.getStorageName())
        .thenCompose(this::createSearchActor)
        .thenCompose(searchActorRef -> sendSearchCommand(request, searchActorRef))
        .thenCompose(status -> {
          if (StringUtils.isNotBlank(status.getSearchId())) {
            return waitAvailableActorRef(status);
          }
          return CompletableFuture.completedFuture(status);
        });
  }

  private CompletionStage<SearchResult> waitAvailableActorRef(SearchResult status) {
    int count = 0;
    boolean allFound = false;

    while (count < RETRY_COUNT && !allFound) {
      try {
        allFound = checkAllNodesReferences(status.getSearchId()).get(2, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.error("The method 'waitAvailableActorRef' was interrupted for search {}", status.getSearchId(), e);
        Thread.currentThread().interrupt();
        count = RETRY_COUNT;
      } catch (Exception e) {
        logger.warn("Cannot find a search reference {}", status.getSearchId());
      }

      if (!allFound) {
        try {
          Thread.sleep(TIMEOUT_MILLIS);
        } catch (InterruptedException e) {
          logger.error("The method 'waitAvailableActorRef' was interrupted in sleep for search {}",
              status.getSearchId(), e);
          Thread.currentThread().interrupt();
          count = RETRY_COUNT;
        }
      }

      count++;
    }

    if (allFound) {
      return CompletableFuture.completedFuture(status);
    } else {
      logger.error("The method 'waitAvailableActorRef' fails for search {} check", status.getSearchId());
      return CompletableFuture.completedFuture(
          new SearchResult.Builder().errorCount(1).searchFinished(true).build());
    }
  }

  CompletableFuture<Boolean> checkAllNodesReferences(String searchId) {
    CompletionStage<Receptionist.Listing> cf = AskPattern.ask(
        actorSystem.receptionist(),
        ref -> Receptionist.find(rootActorsKey, ref),
        Duration.ofMinutes(1),
        actorSystem.scheduler()
    );

    return cf.toCompletableFuture().thenCompose(listing -> {
      List<CompletableFuture<Boolean>> results = listing.getServiceInstances(rootActorsKey)
          .stream()
          .map(rootRef ->
              AskPattern.<MoleculeSourceRootActor.Command, Boolean>askWithStatus(
                  rootRef,
                  ref -> new MoleculeSourceRootActor.CheckActorReference(ref,
                      MoleculeSearchActor.Command.class, searchId),
                  Duration.ofMinutes(1),
                  actorSystem.scheduler()).toCompletableFuture()).toList();

      return CompletableFuture.allOf(results.toArray(new CompletableFuture[0]))
          .thenApply(v -> results.stream().allMatch(CompletableFuture::join));
    });
  }

  public CompletionStage<SearchResult> nextSearchResult(String searchId, int limit, String user) {
    ServiceKey<MoleculeSearchActor.Command> serviceKey = searchActorKey(searchId);

    CompletionStage<Receptionist.Listing> cf = AskPattern.ask(
        actorSystem.receptionist(),
        ref -> Receptionist.find(serviceKey, ref),
        Duration.ofMinutes(1),
        actorSystem.scheduler());

    return cf.toCompletableFuture().thenCompose(listing -> {
      if (listing.getServiceInstances(serviceKey).size() != 1) {
        return CompletableFuture.failedFuture(new RuntimeException("Search not found: " + searchId));
      }
      var searchActorRef = listing.getServiceInstances(serviceKey).iterator().next();
      return sendSearchNext(searchActorRef, limit, user);
    });
  }

  private void validate(SearchRequest request) {
    if (request.getBufferSize() <= 0) {
      throw new IllegalArgumentException("Buffer size must be positive.");
    }

    if (request.getParallelism() <= 0) {
      throw new IllegalArgumentException("Parallelism must be positive.");
    }
  }

  private CompletionStage<SearchResult> sendSearchNext(ActorRef<MoleculeSearchActor.Command> actorRef, int limit,
                                                       String user) {
    return AskPattern.askWithStatus(
        actorRef,
        replyTo -> new MoleculeSearchActor.SearchNext(replyTo, limit, user),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  private CompletionStage<MoleculeSourceRootActor.SourceActorDescription> findSourceActor(String storageName) {
    return listSources()
        .thenApply(sourceActorDescriptions -> sourceActorDescriptions.stream()
            .filter(item -> item.storageName.equals(storageName)).findFirst().orElseThrow());
  }

  private <S> CompletionStage<PipelineStatistics> loadFromDataSource(
      String libraryName, DataSource<S> dataSource, TransformationStep<S, Molecule> transformation,
      ActorRef<MoleculeSourceActor.Command> sourceActorRef) {
    return AskPattern.askWithStatus(
        sourceActorRef,
        replyTo -> new MoleculeSourceActor.LoadFromDataSource<>(libraryName, dataSource, transformation, replyTo),
        //TODO: probably not ideal solution to have long timeout here.
        Duration.ofDays(1),
        actorSystem.scheduler()
    );
  }

  private CompletionStage<ActorRef<MoleculeSourceActor.Command>> createSource(
      String storageName, int maxUploads, StorageType storageType, DataStorage<Molecule> storage) {
    return AskPattern.askWithStatus(
        rootActorRef,
        replyTo -> new MoleculeSourceRootActor.CreateSource(replyTo, storageName, maxUploads, storageType, storage),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  private CompletionStage<List<DataLibrary>> getIndexes(ActorRef<MoleculeSourceActor.Command> actorRef) {
    return AskPattern.askWithStatus(
        actorRef,
        MoleculeSourceActor.GetLibraries::new,
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  private CompletionStage<DataLibrary> sendMessageFindLibrary(ActorRef<MoleculeSourceActor.Command> actorRef,
                                                              String libraryName, DataLibraryType libraryType) {
    return AskPattern.askWithStatus(
        actorRef,
        replyTo -> new MoleculeSourceActor.FindLibrary(replyTo, libraryName, libraryType),
        Duration.ofMinutes(1),
        actorSystem.scheduler()
    );
  }

  private CompletionStage<DataLibrary> createIndex(ActorRef<MoleculeSourceActor.Command> actorRef,
                                                   DataLibrary index) {
    return AskPattern.askWithStatus(
        actorRef,
        replyTo -> new MoleculeSourceActor.CreateLibrary(replyTo, index),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  private CompletionStage<SearchResult> sendSearchCommand(SearchRequest searchRequest,
                                                          ActorRef<MoleculeSearchActor.Command> searchActorRef) {
    return AskPattern.askWithStatus(
        searchActorRef,
        replyTo -> new MoleculeSearchActor.Search(replyTo, searchRequest),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  private CompletionStage<ActorRef<MoleculeSearchActor.Command>> createSearchActor(
      MoleculeSourceRootActor.SourceActorDescription sourceActorDescription) {
    return AskPattern.askWithStatus(
        sourceActorDescription.actorRef,
        MoleculeSourceActor.CreateSearch::new,
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }
}
