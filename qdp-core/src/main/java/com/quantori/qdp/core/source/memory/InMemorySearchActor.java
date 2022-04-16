package com.quantori.qdp.core.source.memory;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.TimerScheduler;
import com.quantori.qdp.core.source.MoleculeSearchActor;
import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import com.quantori.qdp.core.source.model.molecule.search.SearchResult;

public class InMemorySearchActor  extends MoleculeSearchActor {

  private final String searchId;
  private final InMemoryLibraryStorage storage;

  private InMemorySearchActor(ActorContext<Command> context, String storageName, InMemoryLibraryStorage storage,
                              TimerScheduler<Command> timerScheduler) {
    super(context, storageName, timerScheduler);
    this.storage = storage;
    this.searchId = getContext().getSelf().path().name();
  }

  public static Behavior<MoleculeSearchActor.Command> create(String storageName, InMemoryLibraryStorage storage) {
    return Behaviors.setup(ctx ->
        Behaviors.withTimers(timer -> new InMemorySearchActor(ctx, storageName, storage, timer)));
  }

  @Override
  protected SearchResult search(SearchRequest searchRequest) {
    getContext().getLog().trace("Got search initial request: {}", searchRequest);

    if (searchRequest.getStrategy() == SearchRequest.SearchStrategy.PAGE_BY_PAGE) {
      //TODO: implement.
    } else {
      throw new UnsupportedOperationException("Strategy is not implemented yet: " + searchRequest.getStrategy());
    }

    /* if (!matchersBySearchType.containsKey(searchRequest.getType())) {
      throw new IllegalArgumentException("Unknown search type: " + searchRequest.getType());
    }
    var filter = matchersBySearchType.get(searchRequest.getType());

    resultProducer = molecules.stream()
        .filter(molecule -> filter.apply(molecule.getStructure(), searchRequest.getQueryStructure()))
        .map(molecule -> getFlattenedMolecule(storageName, searchRequest.isHydrogenVisible(), molecule)
        ).iterator();

    return collectResponseMolecule(searchRequest.getLimit());*/
    return null;
  }

  @Override
  protected SearchResult searchNext(int limit) {
    //TODO: implement.
    return null;
  }

  @Override
  protected SearchResult searchStatistics() {
    //TODO: implement.
    return null;
  }

  @Override
  protected SearchRequest getSearchRequest() {
    return null;
  }

  @Override
  protected void onTerminate() {
    // No-op.
  }
  /*  private synchronized QDPSearchResult collectResponseMolecule(int limit) {
    List<QDPSearchResultMolecule> found = new ArrayList<>();

    if (limit > 0) {
      while (limit > 0 && resultProducer.hasNext()) {
        found.add(resultProducer.next());
        limit--;
      }
    } else {
      resultProducer.forEachRemaining(found::add);
    }

    return new QDPSearchResult(!resultProducer.hasNext(), searchId, totalFound, found);
  }*/
}
