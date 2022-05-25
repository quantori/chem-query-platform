package com.quantori.qdp.core.source;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Receive;
import akka.pattern.StatusReply;
import com.quantori.qdp.core.source.model.DataLibrary;
import com.quantori.qdp.core.source.model.DataLibraryType;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class ReactionSourceActor extends AbstractBehavior<ReactionSourceActor.Command> {
  protected final String storageName;

  private final Queue<MoleculeSourceActor.LoadFromDataSource<?>> uploadCmdQueue = new LinkedList<>();
  private final int maxUploadCount;
  private final AtomicInteger uploadCount = new AtomicInteger();

  protected ReactionSourceActor(ActorContext<ReactionSourceActor.Command> context, String storageName,
                                int maxUploadCount) {
    super(context);
    this.storageName = storageName;
    if (maxUploadCount <= 0) {
      throw new IllegalArgumentException(
          "Expected max uploads parameter greater then 0 but received " + maxUploadCount);
    }
    this.maxUploadCount = maxUploadCount;
  }

  @Override
  public Receive<ReactionSourceActor.Command> createReceive() {
    return newReceiveBuilder()
        .build();
  }
  protected abstract CompletionStage<DataLibrary> findLibrary(ReactionSourceActor.FindLibrary cmd);
  protected abstract CompletionStage<List<DataLibrary>> getLibraries();

  private Behavior<ReactionSourceActor.Command> onGetLibrary(final ReactionSourceActor.GetLibraries cmd) {
    getLibraries().whenComplete((list, t) -> {
      if (list != null) {
        cmd.replyTo.tell(StatusReply.success(list));
      } else {
        cmd.replyTo.tell(StatusReply.error(t));
      }
    });
    return this;
  }

  private Behavior<ReactionSourceActor.Command> onFindLibrary(ReactionSourceActor.FindLibrary cmd) {
    var log = getContext().getLog();
    log.debug("Received find library command [cmd={}]", cmd);
    findLibrary(cmd).whenComplete((library, t) -> {
      if (t == null) {
        log.debug("Library found [storageName={}, libraryName={}, libraryType={}]", storageName, cmd.libraryName,
            cmd.libraryType);
        cmd.replyTo.tell(StatusReply.success(library));
      } else {
        log.error("Failed to find or create library [storageName={}, libraryName={}, libraryType={}]", storageName,
            cmd.libraryName, cmd.libraryType, t);
        cmd.replyTo.tell(StatusReply.error(t));
      }
    });
    return this;
  }


  public abstract static class Command {
  }

  public static class GetLibraries extends MoleculeSourceActor.Command {
    public final ActorRef<StatusReply<List<DataLibrary>>> replyTo;

    public GetLibraries(ActorRef<StatusReply<List<DataLibrary>>> replyTo) {
      this.replyTo = replyTo;
    }
  }

  public static class FindLibrary extends MoleculeSourceActor.Command {
    public final ActorRef<StatusReply<DataLibrary>> replyTo;
    public final String libraryName;
    public final DataLibraryType libraryType;

    public FindLibrary(final ActorRef<StatusReply<DataLibrary>> replyTo, final String libraryName,
                       final DataLibraryType libraryType) {
      this.replyTo = replyTo;
      this.libraryName = libraryName;
      this.libraryType = libraryType;
    }
  }

}
