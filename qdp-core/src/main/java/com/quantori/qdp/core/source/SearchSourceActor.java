package com.quantori.qdp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import akka.pattern.StatusReply;
import java.util.Map;
import java.util.UUID;

import com.quantori.qdp.core.model.DataStorage;
import com.quantori.qdp.core.model.StorageItem;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class SearchSourceActor<I extends StorageItem> extends AbstractBehavior<SearchSourceActor.Command> {
  private final Map<String, DataStorage<?, I>> storages;

  private SearchSourceActor(ActorContext<Command> context, Map<String, DataStorage<?, I>> storages) {
    super(context);
    this.storages = storages;
  }

  static <I extends StorageItem> Behavior<Command> create(Map<String, DataStorage<?, I>> storages) {
    return Behaviors.setup(ctx -> new SearchSourceActor<>(ctx, storages));
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(CreateSearch.class, this::onCreateSearch)
        .build();
  }


  private Behavior<Command> onCreateSearch(CreateSearch createSearchCmd) {
    String searchId = UUID.randomUUID().toString();
    ActorRef<SearchActor.Command> searchRef = createSearchActor(searchId);
    registerSearchActor(createSearchCmd.replyTo, searchRef, searchId);
    return this;
  }

  private void registerSearchActor(ActorRef<StatusReply<ActorRef<SearchActor.Command>>> replyTo,
                                   ActorRef<SearchActor.Command> searchRef, String searchId) {
    ServiceKey<SearchActor.Command> serviceKey = SearchActor.searchActorKey(searchId);

    Behavior<Receptionist.Registered> listener = Behaviors.receive(Receptionist.Registered.class)
        .onMessage(Receptionist.Registered.class, message -> {
          if (message.getKey().id().equals(searchId)) {
            replyTo.tell(StatusReply.success(searchRef));
            return Behaviors.stopped();
          }

          return Behaviors.same();
        }).build();

    ActorRef<Receptionist.Registered> refListener = getContext().spawn(listener, "registerer-" + UUID.randomUUID());

    getContext().getSystem().receptionist()
        .tell(Receptionist.register(serviceKey, searchRef, refListener));
    getContext().getSystem().receptionist()
        .tell(Receptionist.register(SearchActor.searchActorsKey, searchRef));
  }

  private ActorRef<SearchActor.Command> createSearchActor(String searchId) {
    ActorRef<SearchActor.Command> searchRef = getContext().spawn(
        SearchActor.create(searchId, storages), "search-" + searchId);
    log.info("Created search actor: {}", searchRef);
    return searchRef;
  }

  abstract static class Command {
  }

  @AllArgsConstructor
  static class CreateSearch extends Command {
    public final ActorRef<StatusReply<ActorRef<SearchActor.Command>>> replyTo;
  }

}
