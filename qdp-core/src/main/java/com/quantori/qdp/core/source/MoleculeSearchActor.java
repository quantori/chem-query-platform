package com.quantori.qdp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.actor.typed.receptionist.ServiceKey;
import akka.pattern.StatusReply;
import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import com.quantori.qdp.core.source.model.molecule.search.SearchResult;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class MoleculeSearchActor extends AbstractBehavior<MoleculeSearchActor.Command> {
  private final String timerId = UUID.randomUUID().toString();
  protected final String searchId;
  //TODO: make this configurable.
  private final Duration inactiveSearchTimeout = Duration.ofMinutes(5);

  //TODO: replace by shared executor service.
  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  public static final ServiceKey<Command> searchActorsKey = ServiceKey.create(Command.class, "searchActors");

  public MoleculeSearchActor(ActorContext<Command> context, String searchId,
                             TimerScheduler<MoleculeSearchActor.Command> timer) {
    super(context);
    this.searchId = searchId;

    // Register timer to terminate this actor in case of inactivity longer than timeout.
    timer.startSingleTimer(timerId, new Timeout(), inactiveSearchTimeout);
  }

  public static ServiceKey<Command> searchActorKey(String searchId) {
    return ServiceKey.create(Command.class, Objects.requireNonNull(searchId));
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
    if (!cmd.user.equals(getSearchRequest().getUser())) {
      cmd.replyTo.tell(StatusReply.error("Search request access violation by user " + cmd.user));
    }

    cmd.replyTo.tell(StatusReply.success(getSearchRequest()));

    return Behaviors.withTimers(timer -> {
      timer.startSingleTimer(timerId, new Timeout(), inactiveSearchTimeout);
      return this;
    });
  }

  private Behavior<MoleculeSearchActor.Command> onSearch(Search searchCmd) {
    try {
      getContext().getLog().info("Search is started with ID {} for user: {}", searchId, searchCmd.searchRequest.getUser());
      search(searchCmd.searchRequest).whenComplete((result, error) -> {
        if (error == null) {
          searchCmd.replyTo.tell(StatusReply.success(result));
        } else {
          searchCmd.replyTo.tell(StatusReply.error(error));
          getContext().getLog().error(String.format("Molecule search failed: %s with ID %s for user: %s", searchCmd.searchRequest,
                  searchId, searchCmd.searchRequest.getUser()), error);
        }
      });
    } catch (Exception ex) {
      getContext().getLog().error(String.format("Molecule search failed: %s with ID %s for user: %s", searchCmd.searchRequest,
              searchId, searchCmd.searchRequest.getUser()), ex);
      searchCmd.replyTo.tell(StatusReply.error(ex));
    }

    return Behaviors.withTimers(timer -> {
      timer.startSingleTimer(timerId, new Timeout(), inactiveSearchTimeout);
      return this;
    });
  }

  private Behavior<MoleculeSearchActor.Command> onSearchNext(SearchNext searchCmd) {
    if (!searchCmd.user.equals(getSearchRequest().getUser())) {
      searchCmd.replyTo.tell(StatusReply.error("Search result access violation by user " + searchCmd.user));
    }
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
        getContext().getLog().error(String.format("Molecule search next failed with ID %s for user: %s", searchId,
                getSearchRequest().getUser()), error);
      }
    });

    return Behaviors.withTimers(timer -> {
      timer.startSingleTimer(timerId, new Timeout(), inactiveSearchTimeout);
      return this;
    });
  }

  private Behavior<MoleculeSearchActor.Command> onTimeout(Timeout cmd) {
    getContext().getLog().debug("Reached search actor timeout (will stop actor): " + getContext().getSelf());
    onTerminate();
    return Behaviors.stopped();
  }

  private Behavior<MoleculeSearchActor.Command> onClose(Close cmd) {
    getContext().getLog().info("Close command was received for search Id: {}, user {}", searchId,
            getSearchRequest().getUser());
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

  public interface Command {
  }

  public static class GetSearchRequest implements Command {
    public final ActorRef<StatusReply<SearchRequest>> replyTo;
    public final String user;

    protected GetSearchRequest(ActorRef<StatusReply<SearchRequest>> replyTo, String user) {
      this.replyTo = replyTo;
      this.user = user;
    }
  }

  public static class Search implements Command {
    public final ActorRef<StatusReply<SearchResult>> replyTo;
    public final SearchRequest searchRequest;

    protected Search(ActorRef<StatusReply<SearchResult>> replyTo, SearchRequest searchRequest) {
      this.replyTo = replyTo;
      this.searchRequest = searchRequest;
    }
  }

  public static class SearchNext implements Command {
    public final ActorRef<StatusReply<SearchResult>> replyTo;
    public final int limit;
    public final String user;

    protected SearchNext(ActorRef<StatusReply<SearchResult>> replyTo, int limit, String user) {
      this.replyTo = replyTo;
      this.limit = limit;
      this.user = user;
    }
  }

  public static class Timeout implements Command {
  }

  public static class Close implements Command {
  }
}
