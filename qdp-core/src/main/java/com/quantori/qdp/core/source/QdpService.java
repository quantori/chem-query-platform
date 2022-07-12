package com.quantori.qdp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import com.quantori.qdp.core.source.model.DataLibrary;
import com.quantori.qdp.core.source.model.DataLibraryType;
import com.quantori.qdp.core.source.model.DataSource;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.MultiStorageSearchRequest;
import com.quantori.qdp.core.source.model.PipelineStatistics;
import com.quantori.qdp.core.source.model.SearchResult;
import com.quantori.qdp.core.source.model.StorageItem;
import com.quantori.qdp.core.source.model.TransformationStep;
import com.quantori.qdp.core.source.model.UploadItem;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class QdpService {
  public static final int MAX_SEARCH_ACTORS = 100;
  private final ActorSystem<?> actorSystem;
  private final ActorRef<SourceRootActor.Command> rootActorRef;

  public QdpService() {
    this(ActorSystem.create(SourceRootActor.create(MAX_SEARCH_ACTORS), "qdp-akka-system"));
  }

  public QdpService(ActorSystem<SourceRootActor.Command> system) {
    this.actorSystem = system;
    this.rootActorRef = system;
  }

  public <I extends StorageItem> void registerStorage(DataStorage<I> storage, String storageName) {
    registerStorage(storage, storageName, Integer.MAX_VALUE);
  }

  /**
   * Registers DataStorage instance with given name.
   */
  public <I extends StorageItem> void registerStorage(DataStorage<I> storage, String storageName, int maxUploads) {
    //TODO: add timeout.
    createSource(storageName, maxUploads, storage).toCompletableFuture().join();
  }

  public void registerStorage(Map<String, DataStorage<? extends StorageItem>> storages) {
    createSource(storages).toCompletableFuture().join();
  }

  /**
   * This is responsibility of client to ensure that data source generated object of same type as
   * molecule transformation step expected.
   */
  public CompletionStage<PipelineStatistics> loadStorageItemsFromDataSource(
      String storageName, String libraryName, DataSource<UploadItem> dataSource,
      TransformationStep<UploadItem, StorageItem> transformation) {
    return findUploadSourceActor(storageName)
        .thenCompose(uploadSourceActorDescription ->
            loadFromDataSource(libraryName, dataSource, transformation, uploadSourceActorDescription.actorRef));
  }

  public CompletionStage<List<SourceRootActor.UploadSourceActorDescription>> listSources() {
    return AskPattern.ask(
        rootActorRef,
        SourceRootActor.GetUploadSources::new,
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  public CompletionStage<List<DataLibrary>> getDataStorageIndexes(String storageName) {
    return findUploadSourceActor(storageName).thenCompose(d -> getIndexes(d.actorRef));
  }

  public CompletionStage<DataLibrary> findLibrary(final String storageName, final String libraryName,
                                                  final DataLibraryType libraryType) {
    return findUploadSourceActor(storageName).thenCompose(
        d -> sendMessageFindLibrary(d.actorRef, libraryName, libraryType));
  }

  public CompletionStage<DataLibrary> createDataStorageIndex(String storageName, DataLibrary index) {
    return findUploadSourceActor(storageName)
        .thenCompose(uploadSourceActorDescription -> createIndex(uploadSourceActorDescription.actorRef, index));
  }

  public CompletionStage<SearchResult> search(MultiStorageSearchRequest request) {
    validate(request);

    return findSearchSourceActor()
        .thenCompose(this::createSearchActor)
        .thenCompose(searchActorRef -> sendSearchCommand(request, searchActorRef))
        .thenCompose(searchResult -> {
          if (StringUtils.isBlank(searchResult.getSearchId())) {
            return CompletableFuture.completedFuture(
                SearchResult.builder().errorCount(1).searchFinished(true).build());
          }
          return waitAvailableActorRef(searchResult);
        });
  }

  private CompletionStage<SearchResult> waitAvailableActorRef(SearchResult searchResult) {
    final int RETRY_COUNT = 600;
    final int RETRY_TIMEOUT_MILLIS = 100;

    String searchId = searchResult.getSearchId();
    int count = 0;

    while (count < RETRY_COUNT) {
      try {
        if (checkAllNodesReferences(searchId).get(2, TimeUnit.SECONDS)) {
          return CompletableFuture.completedFuture(searchResult);
        }
      } catch (InterruptedException e) {
        log.error("The method 'waitAvailableActorRef' was interrupted for search {}", searchId, e);
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        log.warn("Cannot find a search reference {}", searchId);
      }

      try {
        Thread.sleep(RETRY_TIMEOUT_MILLIS);
      } catch (InterruptedException e) {
        log.error("The method 'waitAvailableActorRef' was interrupted in sleep for search {}", searchId, e);
        Thread.currentThread().interrupt();
        break;
      }

      count++;
    }
    return CompletableFuture.completedFuture(SearchResult.builder().errorCount(1).searchFinished(true).build());
  }

  CompletableFuture<Boolean> checkAllNodesReferences(String searchId) {
    CompletionStage<Receptionist.Listing> cf = AskPattern.ask(
        actorSystem.receptionist(),
        ref -> Receptionist.find(SourceRootActor.rootActorsKey, ref),
        Duration.ofMinutes(1),
        actorSystem.scheduler()
    );

    return cf.toCompletableFuture().thenCompose(listing -> {
      List<CompletableFuture<Boolean>> results =
          listing.getServiceInstances(SourceRootActor.rootActorsKey).stream()
              .map(rootRef ->
                  AskPattern.<SourceRootActor.Command, Boolean>askWithStatus(
                      rootRef,
                      ref -> new SourceRootActor.CheckActorReference(
                          ref, SearchActor.Command.class, searchId),
                      Duration.ofMinutes(1),
                      actorSystem.scheduler()
                  ).toCompletableFuture())
              .toList();

      return CompletableFuture.allOf(results.toArray(new CompletableFuture[0]))
          .thenApply(v -> results.stream().allMatch(CompletableFuture::join));
    });
  }

  public CompletionStage<SearchResult> nextSearchResult(String searchId, int limit, String user) {
    ServiceKey<SearchActor.Command> serviceKey = SearchActor.searchActorKey(searchId);

    CompletionStage<Receptionist.Listing> findSearchActorRef = AskPattern.ask(
        actorSystem.receptionist(),
        ref -> Receptionist.find(serviceKey, ref),
        Duration.ofMinutes(1),
        actorSystem.scheduler());

    return findSearchActorRef.toCompletableFuture().thenCompose(listing ->
        sendSearchNext(getActorRef(searchId, listing.getServiceInstances(serviceKey)), limit, user));
  }

  private void validate(MultiStorageSearchRequest request) {
    if (request.getProcessingSettings().getBufferSize() <= 0) {
      throw new IllegalArgumentException("Buffer size must be positive.");
    }

    if (request.getProcessingSettings().getParallelism() <= 0) {
      throw new IllegalArgumentException("Parallelism must be positive.");
    }
  }

  private CompletionStage<SearchResult> sendSearchNext(
      ActorRef<com.quantori.qdp.core.source.SearchActor.Command> actorRef, int limit,
      String user) {
    return AskPattern.askWithStatus(
        actorRef,
        replyTo -> new com.quantori.qdp.core.source.SearchActor.SearchNext(replyTo, limit, user),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  private CompletionStage<SourceRootActor.UploadSourceActorDescription> findUploadSourceActor(String storageName) {
    return listSources()
        .thenApply(sourceActorDescriptions -> sourceActorDescriptions.stream()
            .filter(item -> item.storageName.equals(storageName)).findFirst().orElseThrow());
  }

  private CompletionStage<SourceRootActor.SearchSourceActorDescription> findSearchSourceActor() {
    return AskPattern.ask(
        rootActorRef,
        SourceRootActor.GetSearchSource::new,
        Duration.ofMinutes(1),
        actorSystem.scheduler()
    );
  }

  private <U extends UploadItem, I extends StorageItem> CompletionStage<PipelineStatistics> loadFromDataSource(
      String libraryName, DataSource<U> dataSource, TransformationStep<U, I> transformation,
      ActorRef<UploadSourceActor.Command> sourceActorRef) {
    return AskPattern.askWithStatus(
        sourceActorRef,
        replyTo -> new UploadSourceActor.LoadFromDataSource<>(libraryName, dataSource, transformation, replyTo),
        //TODO: probably not ideal solution to have long timeout here.
        Duration.ofDays(1),
        actorSystem.scheduler()
    );
  }

  private CompletionStage<ActorRef<UploadSourceActor.Command>> createSource(
      String storageName, int maxUploads, DataStorage<? extends StorageItem> storage) {
    return AskPattern.askWithStatus(
        rootActorRef,
        replyTo -> new SourceRootActor.CreateUploadSource(replyTo, storageName, maxUploads, storage),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  private CompletionStage<ActorRef<SearchSourceActor.Command>> createSource(
      Map<String, DataStorage<? extends StorageItem>> storages) {
    return AskPattern.askWithStatus(
        rootActorRef,
        replyTo -> new SourceRootActor.CreateSearchSource(replyTo, storages),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  private CompletionStage<List<DataLibrary>> getIndexes(ActorRef<UploadSourceActor.Command> actorRef) {
    return AskPattern.askWithStatus(
        actorRef,
        UploadSourceActor.GetLibraries::new,
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  private CompletionStage<DataLibrary> sendMessageFindLibrary(ActorRef<UploadSourceActor.Command> actorRef,
                                                              String libraryName, DataLibraryType libraryType) {
    return AskPattern.askWithStatus(
        actorRef,
        replyTo -> new UploadSourceActor.FindLibrary(replyTo, libraryName, libraryType),
        Duration.ofMinutes(1),
        actorSystem.scheduler()
    );
  }

  private CompletionStage<DataLibrary> createIndex(
      ActorRef<UploadSourceActor.Command> actorRef, DataLibrary index) {
    return AskPattern.askWithStatus(
        actorRef,
        replyTo -> new UploadSourceActor.CreateLibrary(replyTo, index),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  private CompletionStage<SearchResult> sendSearchCommand(
      MultiStorageSearchRequest searchRequest, ActorRef<SearchActor.Command> searchActorRef) {
    return AskPattern.askWithStatus(
        searchActorRef,
        replyTo -> new SearchActor.Search(replyTo, searchRequest),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  public static ActorRef<SearchActor.Command> getActorRef(
      String searchId, Set<ActorRef<SearchActor.Command>> serviceInstances) {
    if (serviceInstances.size() != 1) {
      throw new RuntimeException("Search not found: " + searchId);
    }
    return serviceInstances.iterator().next();
  }

  private CompletionStage<ActorRef<SearchActor.Command>> createSearchActor(
      SourceRootActor.SearchSourceActorDescription searchSourceActorDescription) {
    return AskPattern.askWithStatus(
        searchSourceActorDescription.actorRef,
        SearchSourceActor.CreateSearch::new,
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }
}
