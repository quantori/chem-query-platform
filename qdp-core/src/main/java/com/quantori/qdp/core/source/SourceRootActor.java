package com.quantori.qdp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import akka.pattern.StatusReply;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.StorageItem;
import com.quantori.qdp.core.utilities.SearchActorsGuardian;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SourceRootActor extends AbstractBehavior<SourceRootActor.Command> {
  private final Map<String, UploadSourceActorDescription> uploadSourceActors = new HashMap<>();
  private final AtomicReference<SearchSourceActorDescription> searchSourceActor = new AtomicReference<>();
  public static final ServiceKey<SourceRootActor.Command> rootActorsKey =
      ServiceKey.create(SourceRootActor.Command.class, "rootActors");

  private SourceRootActor(ActorContext<Command> context, int maxAmountOfSearchActors) {
    super(context);
    context.spawn(SearchActorsGuardian.create(maxAmountOfSearchActors), "SearchActorsGuardian");
    registerRootActor(context);
  }

  public static Behavior<SourceRootActor.Command> create(int maxAmountOfSearchActors) {
    return Behaviors.setup(context -> new SourceRootActor(context, maxAmountOfSearchActors));
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(CreateUploadSource.class, this::onCreateUploadSource)
        .onMessage(GetUploadSources.class, this::onGetUploadSources)
        .onMessage(CreateSearchSource.class, this::onCreateSearchSource)
        .onMessage(GetSearchSource.class, this::onGetSearchSource)
        .onMessage(CheckActorReference.class, this::onCheckActorReference)
        .onMessage(StartActor.class, this::onStartActor)
        .build();
  }

  private <T> Behavior<Command> onStartActor(StartActor<T> startActor) {
    ActorRef<T> actorRef = getContext().spawn(startActor.actor, "child-" + UUID.randomUUID());
    startActor.replyTo.tell(new StartedActor<>(actorRef));
    return this;
  }

  private Behavior<Command> onCheckActorReference(CheckActorReference command) {
    ServiceKey<?> serviceKey = ServiceKey.create(command.cmdClass, Objects.requireNonNull(command.id));

    checkActorRef(serviceKey).whenComplete((success, throwable) -> {
      if (Objects.nonNull(throwable)) {
        command.replyTo.tell(StatusReply.error(throwable));
      } else {
        command.replyTo.tell(StatusReply.success(success));
      }
    });

    return this;
  }

  private CompletionStage<Boolean> checkActorRef(ServiceKey<?> serviceKey) {
    CompletionStage<Receptionist.Listing> cf = AskPattern.ask(
        getContext().getSystem().receptionist(),
        ref -> Receptionist.find(serviceKey, ref),
        Duration.ofMinutes(1),
        getContext().getSystem().scheduler());

    return cf.toCompletableFuture().thenApply(listing ->
        !listing.getServiceInstances(serviceKey).isEmpty());
  }

  private void registerRootActor(ActorContext<Command> context) {
    context.getSystem()
        .receptionist()
        .tell(Receptionist.register(rootActorsKey, context.getSelf()));
  }

  private Behavior<SourceRootActor.Command> onCreateUploadSource(CreateUploadSource createUploadSourceCmd) {
    if (uploadSourceActors.containsKey(createUploadSourceCmd.storageName)) {
      createUploadSourceCmd.replyTo.tell(StatusReply.error("Storage name already in use"));
    }

    ActorRef<UploadSourceActor.Command> searchRef;
    searchRef = getContext().spawn(
        UploadSourceActor.create(createUploadSourceCmd.storage, createUploadSourceCmd.maxUploads),
        "source-" + createUploadSourceCmd.storageName);


    log.info("Created source actor: {}", searchRef);
    uploadSourceActors.put(createUploadSourceCmd.storageName,
        new UploadSourceActorDescription(createUploadSourceCmd.storageName, searchRef));
    createUploadSourceCmd.replyTo.tell(StatusReply.success(searchRef));
    return this;
  }

  private Behavior<SourceRootActor.Command> onGetUploadSources(GetUploadSources getUploadSources) {
    getUploadSources.replyTo.tell(new ArrayList<>(uploadSourceActors.values()));
    return this;
  }

  protected Behavior<Command> onCreateSearchSource(CreateSearchSource createSearchSourceCmd) {
    if (searchSourceActor.get() != null) {
      createSearchSourceCmd.replyTo.tell(StatusReply.error("MultiStorage already created"));
    }
    ActorRef<SearchSourceActor.Command> searchRef;
    searchRef = getContext().spawn(
        SearchSourceActor.create(createSearchSourceCmd.storages),
        "source-multi-storage");
    log.info("Created source actor: {}", searchRef);
    this.searchSourceActor.set(new SearchSourceActorDescription(searchRef));
    createSearchSourceCmd.replyTo.tell(StatusReply.success(searchRef));
    return this;
  }

  protected Behavior<Command> onGetSearchSource(GetSearchSource getSearchSourceCmd) {
    getSearchSourceCmd.replyTo.tell(searchSourceActor.get());
    return this;
  }

  public interface Command {
  }

  @Value
  public static class CheckActorReference implements Command {
    ActorRef<StatusReply<Boolean>> replyTo;
    Class<?> cmdClass;
    String id;
  }

  @AllArgsConstructor
  public static class CreateUploadSource implements Command {
    public final ActorRef<StatusReply<ActorRef<UploadSourceActor.Command>>> replyTo;
    public final String storageName;
    public final int maxUploads;
    public final DataStorage storage;
  }

  @AllArgsConstructor
  public static class GetUploadSources implements Command {
    public final ActorRef<List<UploadSourceActorDescription>> replyTo;
  }

  @AllArgsConstructor
  public static class UploadSourceActorDescription {
    public final String storageName;
    public final ActorRef<UploadSourceActor.Command> actorRef;
  }

  @AllArgsConstructor
  public static class CreateSearchSource implements Command {
    public final ActorRef<StatusReply<ActorRef<SearchSourceActor.Command>>> replyTo;
    public final Map<String, DataStorage<?>> storages;
  }

  @AllArgsConstructor
  public static class GetSearchSource implements Command {
    public final ActorRef<SearchSourceActorDescription> replyTo;
  }

  @AllArgsConstructor
  public static class SearchSourceActorDescription {
    public final ActorRef<SearchSourceActor.Command> actorRef;
  }

  @AllArgsConstructor
  public static class StartedActor<T> {
    public final ActorRef<T> actorRef;
  }

  @AllArgsConstructor
  public static class StartActor<T> implements Command {
    public final Behavior<T> actor;
    public final ActorRef<StartedActor<T>> replyTo;
  }
}
