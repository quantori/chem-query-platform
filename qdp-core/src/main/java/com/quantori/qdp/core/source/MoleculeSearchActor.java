package com.quantori.qdp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import akka.pattern.StatusReply;
import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import com.quantori.qdp.core.source.model.molecule.search.SearchResult;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class MoleculeSearchActor extends AbstractBehavior<MoleculeSearchActor.Command> {
  protected final String storageName;
  private final String timerId = UUID.randomUUID().toString();
  protected final String searchId;
  //TODO: make this configurable.
  private final Duration inactiveSearchTimeout = Duration.ofMinutes(1);

  //TODO: replace by shared executor service.
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  public static final ServiceKey<Command> searchActorsKey = ServiceKey.create(Command.class, "searchActors");

  public MoleculeSearchActor(ActorContext<Command> context, String storageName,
                             TimerScheduler<MoleculeSearchActor.Command> timer) {
    super(context);
    this.storageName = storageName;
    this.searchId = UUID.randomUUID().toString();

    // Register timer to terminate this actor in case of inactivity longer than timeout.
    timer.startSingleTimer(timerId, new Timeout(), inactiveSearchTimeout);

    // Register this actor in Receptionist to make it globally discoverable.
    context.getSystem().receptionist().tell(Receptionist.register(searchActorKey(searchId), context.getSelf()));
    context.getSystem().receptionist().tell(Receptionist.register(searchActorsKey, context.getSelf()));
  }

  public static ServiceKey<Command> searchActorKey(String searchId) {
    return ServiceKey.create(Command.class, searchId);
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(MoleculeSearchActor.Search.class, this::onSearch)
        .onMessage(MoleculeSearchActor.SearchNext.class, this::onSearchNext)
        .onMessage(MoleculeSearchActor.GetSearchRequest.class, this::onGetSearchRequest)
        .onMessage(Timeout.class, this::onTimeout)
        .onMessage(Close.class, this::onClose)
        .build();
  }

  private Behavior<Command> onGetSearchRequest(GetSearchRequest cmd) {
    cmd.replyTo.tell(StatusReply.success(getSearchRequest()));

    return Behaviors.withTimers(timer -> {
      timer.startSingleTimer(timerId, new Timeout(), inactiveSearchTimeout);
      return this;
    });
  }

  private Behavior<MoleculeSearchActor.Command> onSearch(Search searchCmd) {
    try {
      search(searchCmd.searchRequest).whenComplete((result, error) -> {
        if (error == null) {
          searchCmd.replyTo.tell(StatusReply.success(result));
        } else {
          searchCmd.replyTo.tell(StatusReply.error(error));
          getContext().getLog().error("Molecule search failed: " + searchCmd.searchRequest, error);
        }
      });
    } catch (Exception ex) {
      getContext().getLog().error("Molecule search failed: " + searchCmd.searchRequest, ex);
      searchCmd.replyTo.tell(StatusReply.error(ex));
    }
    //TODO: We don't stop actor here if search is completed

    return Behaviors.withTimers(timer -> {
      timer.startSingleTimer(timerId, new Timeout(), inactiveSearchTimeout);
      return this;
    });
  }

  private Behavior<MoleculeSearchActor.Command> onSearchNext(SearchNext searchCmd) {
    CompletionStage<SearchResult> searchResult;
    if (searchCmd.limit > 0) {
      searchResult = searchNext(searchCmd.limit);
    } else {
      searchResult = searchStatistics();
    }
    searchResult.whenComplete((result, error) -> {
      if (error == null) {
        searchCmd.replyTo.tell(StatusReply.success(result));
      } else {
        searchCmd.replyTo.tell(StatusReply.error(error.getMessage()));
        getContext().getLog().error("Molecule search next failed", error);
      }
    });

    return Behaviors.withTimers(timer -> {
      timer.startSingleTimer(timerId, new Timeout(), inactiveSearchTimeout);
      return this;
    });
  }

  private Behavior<MoleculeSearchActor.Command> onTimeout(Timeout cmd) {
    getContext().getLog().info("Reached search actor timeout (will stop actor): " + getContext().getSelf());
    onTerminate();
    return Behaviors.stopped();
  }

  private Behavior<MoleculeSearchActor.Command> onClose(Close cmd) {
    getContext().getLog().info("Close command was received for actor: " + getContext().getSelf());
    onTerminate();
    return Behaviors.stopped();
  }

  protected abstract CompletionStage<SearchResult> search(SearchRequest searchRequest);

  protected abstract CompletionStage<SearchResult> searchNext(int limit);

  protected abstract CompletionStage<SearchResult> searchStatistics();

  protected abstract SearchRequest getSearchRequest();

  protected abstract void onTerminate();

  protected ExecutorService getExecutor() {
    return executor;
  }

  public abstract static class Command {
  }

  public static class GetSearchRequest extends Command {
    public final ActorRef<StatusReply<SearchRequest>> replyTo;

    protected GetSearchRequest(ActorRef<StatusReply<SearchRequest>> replyTo) {
      this.replyTo = replyTo;
    }
  }

  public static class Search extends Command {
    public final ActorRef<StatusReply<SearchResult>> replyTo;
    public final SearchRequest searchRequest;

    protected Search(ActorRef<StatusReply<SearchResult>> replyTo, SearchRequest searchRequest) {
      this.replyTo = replyTo;
      this.searchRequest = searchRequest;
    }
  }

  public static class SearchNext extends Command {
    public final ActorRef<StatusReply<SearchResult>> replyTo;
    public final int limit;

    protected SearchNext(ActorRef<StatusReply<SearchResult>> replyTo, int limit) {
      this.replyTo = replyTo;
      this.limit = limit;
    }
  }

  public static class Timeout extends Command {
  }

  public static class Close extends Command {
  }
}
