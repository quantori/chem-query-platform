package com.quantori.qdp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import com.quantori.qdp.api.model.core.DataSource;
import com.quantori.qdp.api.model.core.DataStorage;
import com.quantori.qdp.api.model.core.DataUploadItem;
import com.quantori.qdp.api.model.core.ErrorType;
import com.quantori.qdp.api.model.core.MultiStorageSearchRequest;
import com.quantori.qdp.api.model.core.PipelineStatistics;
import com.quantori.qdp.api.model.core.SearchError;
import com.quantori.qdp.api.model.core.SearchItem;
import com.quantori.qdp.api.model.core.SearchResult;
import com.quantori.qdp.api.model.core.StorageItem;
import com.quantori.qdp.api.model.core.StorageRequest;
import com.quantori.qdp.api.model.core.StorageUploadItem;
import com.quantori.qdp.api.model.core.TransformationStep;
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
public class QdpService<D extends DataUploadItem, U extends StorageUploadItem, S extends SearchItem, I extends StorageItem> {
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

  public static ActorRef<SearchActor.Command> getActorRef(
      String searchId, Set<ActorRef<SearchActor.Command>> serviceInstances) {
    if (serviceInstances.size() != 1) {
      throw new IllegalArgumentException("Search not found: " + searchId);
    }
    return serviceInstances.iterator().next();
  }

  public void registerUploadStorage(DataStorage<U, ?, ?> storage, String storageName) {
    registerUploadStorage(storage, storageName, Integer.MAX_VALUE);
  }

  /**
   * Registers DataStorage instance with given name.
   */
  public void registerUploadStorage(DataStorage<U, ?, ?> storage, String storageName, int maxUploads) {
    //TODO: add timeout.
    createSource(storageName, maxUploads, storage).toCompletableFuture().join();
  }

  public void registerSearchStorages(Map<String, DataStorage<?, S, I>> storages) {
    createSource(storages).toCompletableFuture().join();
  }

  public CompletionStage<List<SourceRootActor.UploadSourceActorDescription>> listSources() {
    return AskPattern.ask(
        rootActorRef,
        SourceRootActor.GetUploadSources::new,
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  /**
   * This is responsibility of client to ensure that data source generated object of same type as
   * molecule transformation step expected.
   */
  public CompletionStage<PipelineStatistics> loadStorageItemsFromDataSource(
      String storageName, String libraryId, DataSource<D> dataSource,
      TransformationStep<D, U> transformation) {
    return findUploadSourceActor(storageName)
        .thenCompose(uploadSourceActorDescription ->
            loadFromDataSource(libraryId, dataSource, transformation, uploadSourceActorDescription.actorRef));
  }

  public CompletionStage<SearchResult<S>> search(MultiStorageSearchRequest<S, I> request) {
    validate(request);

    return findSearchSourceActor()
        .thenCompose(this::createSearchActor)
        .thenCompose(searchActorRef -> sendSearchCommand(request, searchActorRef))
        .thenCompose(searchResult -> {
          if (StringUtils.isBlank(searchResult.getSearchId())) {
            return CompletableFuture.completedFuture(
                SearchResult.<S>builder()
                    .errors(List.of(new SearchError(ErrorType.GENERAL, "undefined", List.of("undefined"),
                        "Unable to obtain searchId")))
                    .searchFinished(true)
                    .build());
          }
          return waitAvailableActorRef(searchResult);
        });
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

  private CompletionStage<SearchResult<S>> waitAvailableActorRef(SearchResult<S> searchResult) {
    final int RETRY_COUNT = 300;
    final int RETRY_TIMEOUT_MILLIS = 100;
    final int NODE_AWAIT_TIMEOUT_MILLIS = 1000;

    String searchId = searchResult.getSearchId();
    int count = 0;

    while (count < RETRY_COUNT) {
      try {
        if (checkAllNodesReferences(searchId).get(NODE_AWAIT_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
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
    return CompletableFuture.completedFuture(SearchResult.<S>builder()
        .errors(List.of(
            new SearchError(ErrorType.GENERAL, "undefined", List.of("undefined"),
                "Unable to find available node to process request")))
        .searchFinished(true)
        .build());
  }

  public void abortSearch(String searchId, String user) {
    ServiceKey<SearchActor.Command> serviceKey = SearchActor.searchActorKey(searchId);

    CompletionStage<Receptionist.Listing> findSearchActorRef = AskPattern.ask(
        actorSystem.receptionist(),
        ref -> Receptionist.find(serviceKey, ref),
        Duration.ofMinutes(1),
        actorSystem.scheduler());

    findSearchActorRef.toCompletableFuture().thenCompose(listing -> {
      if (listing.getServiceInstances(serviceKey).size() != 1) {
        return CompletableFuture.failedFuture(new RuntimeException("Search not found: " + searchId));
      }
      var searchActorRef = listing.getServiceInstances(serviceKey).iterator().next();
      return AskPattern.askWithStatus(
          searchActorRef,
          ref -> new SearchActor.Close(user),
          Duration.ofMinutes(1),
          actorSystem.scheduler());
    });
  }

  public CompletionStage<StorageRequest> getSearchRequestDescription(String searchId, String storage, String user) {
    ServiceKey<SearchActor.Command> serviceKey = SearchActor.searchActorKey(searchId);

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
          ref -> new SearchActor.GetSearchRequest(ref, storage, user),
          Duration.ofMinutes(1),
          actorSystem.scheduler());
    });
  }

  public CompletionStage<SearchResult<S>> nextSearchResult(
      String searchId, int limit, String user) {
    ServiceKey<SearchActor.Command> serviceKey = SearchActor.searchActorKey(searchId);

    CompletionStage<Receptionist.Listing> findSearchActorRef = AskPattern.ask(
        actorSystem.receptionist(),
        ref -> Receptionist.find(serviceKey, ref),
        Duration.ofMinutes(1),
        actorSystem.scheduler());

    return findSearchActorRef.toCompletableFuture().thenCompose(listing ->
        sendSearchNext(getActorRef(searchId, listing.getServiceInstances(serviceKey)), limit, user));
  }

  private void validate(MultiStorageSearchRequest<S, I> request) {
    if (request.getProcessingSettings().getBufferSize() <= 0) {
      throw new IllegalArgumentException("Buffer size must be positive.");
    }

    if (request.getProcessingSettings().getParallelism() <= 0) {
      throw new IllegalArgumentException("Parallelism must be positive.");
    }
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

  private CompletionStage<SearchResult<S>> sendSearchNext(
      ActorRef<SearchActor.Command> actorRef, int limit, String user) {
    return AskPattern.askWithStatus(
        actorRef,
        replyTo -> new SearchActor.SearchNext<>(replyTo, limit, user),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  private CompletionStage<PipelineStatistics> loadFromDataSource(
      String libraryId, DataSource<D> dataSource, TransformationStep<D, U> transformation,
      ActorRef<UploadSourceActor.Command> sourceActorRef) {
    return AskPattern.askWithStatus(
        sourceActorRef,
        replyTo -> new UploadSourceActor.LoadFromDataSource<>(libraryId, dataSource, transformation, replyTo),
        //TODO: probably not ideal solution to have long timeout here.
        Duration.ofDays(1),
        actorSystem.scheduler()
    );
  }

  private CompletionStage<ActorRef<UploadSourceActor.Command>> createSource(
      String storageName, int maxUploads, DataStorage<U, ?, ?> storage) {
    return AskPattern.askWithStatus(
        rootActorRef,
        replyTo -> new SourceRootActor.CreateUploadSource<>(replyTo, storageName, maxUploads, storage),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  private CompletionStage<ActorRef<SearchSourceActor.Command>> createSource(
      Map<String, DataStorage<?, S, I>> storages) {
    return AskPattern.askWithStatus(
        rootActorRef,
        replyTo -> new SourceRootActor.CreateSearchSource<>(replyTo, storages),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  private CompletionStage<SearchResult<S>> sendSearchCommand(
      MultiStorageSearchRequest<S, I> searchRequest, ActorRef<SearchActor.Command> searchActorRef) {
    return AskPattern.askWithStatus(
        searchActorRef,
        replyTo -> new SearchActor.Search<>(replyTo, searchRequest),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
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
