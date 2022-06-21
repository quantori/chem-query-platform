package com.quantori.qdp.core.source.external;

import akka.NotUsed;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.pattern.StatusReply;
import akka.stream.ActorAttributes;
import akka.stream.Attributes;
import akka.stream.FlowShape;
import akka.stream.Supervision;
import akka.stream.UniformFanInShape;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Balance;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Merge;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.typed.javadsl.ActorSink;
import com.quantori.qdp.core.source.model.DataSearcher;
import com.quantori.qdp.core.source.model.molecule.search.SearchRequest;
import com.quantori.qdp.core.source.model.molecule.search.SearchResultItem;
import com.quantori.qdp.core.source.model.molecule.search.StorageResultItem;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataSourceActor extends AbstractBehavior<DataSourceActor.Command> {
  private static final Logger logger = LoggerFactory.getLogger(DataSourceActor.class);
  private final SearchRequest searchRequest;
  private final DataSearcher dataSearcher;
  private final AtomicLong errorCounter = new AtomicLong(0);
  private final AtomicLong foundByStorageCount = new AtomicLong(0);
  private final AtomicLong matchedCount = new AtomicLong(0);
  private final AtomicBoolean sourceIsEmpty = new AtomicBoolean(false);
  private final ActorRef<BufferSinkActor.Command> bufferActorSinkRef;

  private DataSourceActor(ActorContext<Command> context, DataSearcher dataSearcher,
                          SearchRequest searchRequest, ActorRef<BufferSinkActor.Command> bufferActorSinkRef) {
    super(context);
    this.dataSearcher = dataSearcher;
    this.searchRequest = searchRequest;
    this.bufferActorSinkRef = bufferActorSinkRef;
    runFlow();
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(CloseFlow.class, this::onCloseFlow)
        .onMessage(StatusFlow.class, this::onStatusFlow)
        .onMessage(CompletedFlow.class, this::onCompletedFlow)
        .build();
  }

  private Behavior<Command> onCompletedFlow(CompletedFlow cmd) {
    logger.debug("The part of the flow was completed");
    sourceIsEmpty.set(true);
    return this;
  }

  private Behavior<Command> onStatusFlow(StatusFlow cmd) {
    cmd.replyTo.tell(StatusReply.success(new StatusResponse(
        sourceIsEmpty.get(),
        errorCounter.get(), foundByStorageCount.get(), matchedCount.get())));
    return this;
  }

  private Behavior<Command> onCloseFlow(CloseFlow message) {
    logger.debug("The flow actor was asked to stop");
    return Behaviors.stopped();
  }

  private void runFlow() {
    logger.debug("The flow was asked to run");
    Source<StorageResultItem, NotUsed> source = getSource();

    Source<SearchResultItem, NotUsed> transStep = addFlowStep(source, errorCounter);

    final Sink<SearchResultItem, NotUsed> sink = getSink();

    transStep
        .alsoTo(Sink.foreach(i -> matchedCount.incrementAndGet()))
        .toMat(sink, Keep.right())
        .withAttributes(ActorAttributes.withSupervisionStrategy(Supervision.getResumingDecider()))
        .run(getContext().getSystem());
    logger.debug("Flow was created");
  }

  private Source<StorageResultItem, NotUsed> getSource() {
    return Source.fromIterator(() -> new Iterator<StorageResultItem>() {
      Iterator<? extends StorageResultItem> data = dataSearcher.next().iterator();

      @Override
      public boolean hasNext() {
        if (!data.hasNext()) {
          var nextBatch = dataSearcher.next();
          data = nextBatch.iterator();
        }
        return data.hasNext();
      }

      @Override
      public StorageResultItem next() {
        foundByStorageCount.incrementAndGet();
        return data.next();
      }
    }).withAttributes(ActorAttributes.withSupervisionStrategy(Supervision.getStoppingDecider()));
  }

  private Source<SearchResultItem, NotUsed> addFlowStep(Source<StorageResultItem, NotUsed> source,
                                                        AtomicLong countOfErrors) {
    var wrappedStep = wrapStep(searchRequest.getRequestStructure().getResultTransformer(), countOfErrors);
    var filterStep = filterStep(searchRequest.getRequestStructure().getResultFilter(), countOfErrors);

    return source.via(
        balancer(Flow.of(StorageResultItem.class)
                .filter(filterStep::test)
                .map(wrappedStep::apply).addAttributes(Attributes.inputBuffer(1, 1)),
            searchRequest.getProcessingSettings().getParallelism())
    );
  }

  private Sink<SearchResultItem, NotUsed> getSink() {
    final BufferSinkActor.StreamCompleted completeMessage = new BufferSinkActor.StreamCompleted(getContext().getSelf());
    return ActorSink.actorRefWithBackpressure(
        bufferActorSinkRef,
        (replyTo, item) -> new BufferSinkActor.Item(replyTo, item, getContext().getSelf()),
        BufferSinkActor.StreamInitialized::new,
        BufferSinkActor.Ack.INSTANCE,
        completeMessage,
        BufferSinkActor.StreamFailure::new
    );
  }

  public static Behavior<Command> create(DataSearcher dataSearcher,
                                         SearchRequest searchRequest,
                                         ActorRef<BufferSinkActor.Command> bufferActorSinkRef) {
    return Behaviors.setup(ctx -> new DataSourceActor(ctx, dataSearcher,
        searchRequest, bufferActorSinkRef));
  }

  @SuppressWarnings("unchecked")
  public static Flow<StorageResultItem, SearchResultItem, NotUsed> balancer(
      Flow<StorageResultItem, SearchResultItem, NotUsed> worker, int workerCount) {
    return Flow.fromGraph(
        GraphDSL.create(
            b -> {
              final UniformFanOutShape<StorageResultItem, StorageResultItem> balance =
                  b.add(Balance.create(workerCount, true));
              final UniformFanInShape<SearchResultItem, SearchResultItem> merge = b.add(Merge.create(workerCount));

              for (int i = 0; i < workerCount; i++) {
                b.from(balance.out(i)).via(b.add(worker.async())).toInlet(merge.in(i));
              }

              return FlowShape.of(balance.in(), merge.out());
            })).addAttributes(Attributes.inputBuffer(1, 1));
  }

  private Predicate<StorageResultItem> filterStep(
      Predicate<StorageResultItem> filter, AtomicLong countOfErrors) {
    return (t) -> {
      try {
        return filter.test(t);
      } catch (RuntimeException e) {
        logger.error("Molecule filter step failed to process data: {}", t, e);
        countOfErrors.incrementAndGet();
        throw e;
      }
    };
  }

  private Function<StorageResultItem, SearchResultItem> wrapStep(
      Function<StorageResultItem, SearchResultItem> transformation, AtomicLong countOfErrors) {
    return t -> {
      try {
        return transformation.apply(t);
      } catch (RuntimeException e) {
        logger.error("Molecule transformation step failed to process data: {}", t, e);
        countOfErrors.incrementAndGet();
        throw e;
      }
    };
  }

  public interface Command {
  }

  public static class CompletedFlow implements Command {
  }

  @Value
  public static class StatusFlow implements Command {
    ActorRef<StatusReply<StatusResponse>> replyTo;
  }


  @Value
  public static class StatusResponse implements Command {
    boolean completed;
    long errorCount;
    long foundByStorageCount;
    long matchedCount;
  }

  public static class CloseFlow implements Command {
  }
}
