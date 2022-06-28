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
import com.quantori.qdp.core.source.model.StorageType;
import com.quantori.qdp.core.source.model.molecule.Molecule;
import com.quantori.qdp.core.utilities.SearchActorsGuardian;
import lombok.Value;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletionStage;

public class MoleculeSourceRootActor extends AbstractBehavior<MoleculeSourceRootActor.Command> {
  private final Map<String, SourceActorDescription> sourceActors = new HashMap<>();
  public static final ServiceKey<MoleculeSourceRootActor.Command> rootActorsKey =
      ServiceKey.create(MoleculeSourceRootActor.Command.class, "rootActors");

  private MoleculeSourceRootActor(ActorContext<Command> context, int maxAmountOfSearchActors) {
    super(context);
    context.spawn(SearchActorsGuardian.create(maxAmountOfSearchActors), "SearchActorsGuardian");
    registerRootActor(context);
  }

  public static Behavior<MoleculeSourceRootActor.Command> create(int maxAmountOfSearchActors) {
    return Behaviors.setup(context -> new MoleculeSourceRootActor(context, maxAmountOfSearchActors));
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(CreateSource.class, this::onCreateSource)
        .onMessage(GetSources.class, this::onGetSources)
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
    ServiceKey serviceKey = ServiceKey.create(command.cmdClass, Objects.requireNonNull(command.id));

    checkActorRef(serviceKey).whenComplete((success, t) -> {
      if (Objects.nonNull(t)) {
        command.replyTo.tell(StatusReply.error(t));
      } else {
        command.replyTo.tell(StatusReply.success(success));
      }
    });

    return this;
  }

  private CompletionStage<Boolean> checkActorRef(ServiceKey serviceKey) {
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

  protected Behavior<MoleculeSourceRootActor.Command> onCreateSource(CreateSource createSourceCmd) {
    if (sourceActors.containsKey(createSourceCmd.storageName)) {
      createSourceCmd.replyTo.tell(StatusReply.error("Storage name already in use"));
    }

    ActorRef<MoleculeSourceActor.Command> searchRef;
    if (createSourceCmd.storageType == StorageType.EXTERNAL) {
      searchRef = getContext().spawn(
          MoleculeSourceActor.create(StorageType.EXTERNAL, createSourceCmd.storageName,
              createSourceCmd.maxUploads, createSourceCmd.storage),
          "source-" + createSourceCmd.storageName);
    } else {
      searchRef = getContext().spawn(MoleculeSourceActor.create(
              createSourceCmd.storageType, createSourceCmd.storageName, createSourceCmd.maxUploads
          ), "source-" + createSourceCmd.storageName
      );
    }

    getContext().getLog().info("Created source actor: {}", searchRef);
    sourceActors.put(createSourceCmd.storageName,
        new SourceActorDescription(createSourceCmd.storageName, searchRef));
    createSourceCmd.replyTo.tell(StatusReply.success(searchRef));
    return this;
  }

  protected Behavior<MoleculeSourceRootActor.Command> onGetSources(GetSources getSources) {
    getSources.replyTo.tell(new ArrayList<>(sourceActors.values()));
    return this;
  }

  public interface Command {
  }

  @Value
  public static class CheckActorReference implements Command {
    ActorRef<StatusReply<Boolean>> replyTo;
    Class cmdClass;
    String id;
  }

  public static class CreateSource implements Command {
    public final ActorRef<StatusReply<ActorRef<MoleculeSourceActor.Command>>> replyTo;
    public final String storageName;
    public final int maxUploads;
    public final StorageType storageType;
    public final DataStorage<Molecule> storage;

    public CreateSource(ActorRef<StatusReply<ActorRef<MoleculeSourceActor.Command>>> replyTo, String storageName,
                        final int maxUploads, StorageType storageType, DataStorage<Molecule> storage) {
      this.replyTo = replyTo;
      this.storageName = storageName;
      this.storageType = storageType;
      this.maxUploads = maxUploads;
      this.storage = storage;
    }
  }

  public static class GetSources implements Command {
    public final ActorRef<List<SourceActorDescription>> replyTo;

    public GetSources(ActorRef<List<SourceActorDescription>> replyTo) {
      this.replyTo = replyTo;
    }
  }

  public static class SourceActorDescription {
    public final String storageName;
    public final ActorRef<MoleculeSourceActor.Command> actorRef;

    public SourceActorDescription(String storageName, ActorRef<MoleculeSourceActor.Command> actorRef) {
      this.storageName = storageName;
      this.actorRef = actorRef;
    }
  }

  public static class StartedActor<T> {
    public final ActorRef<T> actorRef;

    public StartedActor(ActorRef<T> actorRef) {
      this.actorRef = actorRef;
    }
  }

  public static class StartActor<T> implements Command {

    public final Behavior<T> actor;
    public final ActorRef<StartedActor<T>> replyTo;

    public StartActor(Behavior<T> actor, ActorRef<StartedActor<T>> replyTo) {
      this.actor = actor;
      this.replyTo = replyTo;
    }
  }
}
