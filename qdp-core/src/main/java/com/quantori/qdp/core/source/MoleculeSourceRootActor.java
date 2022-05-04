package com.quantori.qdp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.pattern.StatusReply;
import com.quantori.qdp.core.source.model.DataStorage;
import com.quantori.qdp.core.source.model.StorageType;
import com.quantori.qdp.core.source.model.molecule.Molecule;
import com.quantori.qdp.core.utilities.SearchActorsGuardian;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MoleculeSourceRootActor extends AbstractBehavior<MoleculeSourceRootActor.Command> {
  //TODO: make this configurable.
  private final int maxAmountOfSearchActors = 100;
  private final Map<String, SourceActorDescription> sourceActors = new HashMap<>();

  public static Behavior<MoleculeSourceRootActor.Command> create() {
    return Behaviors.setup(MoleculeSourceRootActor::new);
  }

  protected MoleculeSourceRootActor(ActorContext<Command> context) {
    super(context);
    context.spawn(SearchActorsGuardian.create(maxAmountOfSearchActors), "SearchActorsGuardian");
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(CreateSource.class, this::onCreateSource)
        .onMessage(GetSources.class, this::onGetSources)
        .build();
  }

  protected Behavior<MoleculeSourceRootActor.Command> onCreateSource(CreateSource createSourceCmd) {
    if (sourceActors.containsKey(createSourceCmd.storageName)) {
      createSourceCmd.replyTo.tell(StatusReply.error("Storage name already in use"));
    }

    ActorRef<MoleculeSourceActor.Command> searchRef;
    if (createSourceCmd.storageType == StorageType.EXTERNAL) {
      searchRef = getContext().spawn(
          MoleculeSourceActor.create(createSourceCmd.storageType, createSourceCmd.storageName,
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

  public interface Command {}

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
}
