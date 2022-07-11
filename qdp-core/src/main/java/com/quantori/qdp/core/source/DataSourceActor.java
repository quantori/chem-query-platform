package com.quantori.qdp.core.source;

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
import com.quantori.qdp.core.source.model.DataSearcher;
import com.quantori.qdp.core.source.model.MultiStorageSearchRequest;
import com.quantori.qdp.core.source.model.RequestStructure;
import com.quantori.qdp.core.source.model.SearchItem;
import com.quantori.qdp.core.source.model.StorageItem;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataSourceActor extends AbstractBehavior<DataSourceActor.Command> {
  private final MultiStorageSearchRequest multiStorageSearchRequest;
  private final Map<String, DataSearcher> dataSearchers;
  private final AtomicLong errorCounter = new AtomicLong(0);
  private final AtomicLong foundByStorageCount = new AtomicLong(0);
  private final AtomicLong matchedCount = new AtomicLong(0);
  private final AtomicBoolean sourceIsEmpty = new AtomicBoolean(false);
  private final ActorRef<BufferSinkActor.Command> bufferActorSinkRef;

  private DataSourceActor(ActorContext<Command> context, Map<String, DataSearcher> dataSearchers,
                          MultiStorageSearchRequest multiStorageSearchRequest,
                          ActorRef<BufferSinkActor.Command> bufferActorSinkRef) {
    super(context);
    this.dataSearchers = dataSearchers;
    this.multiStorageSearchRequest = multiStorageSearchRequest;
    this.bufferActorSinkRef = bufferActorSinkRef;
    runFlow();
  }

  public static Behavior<Command> create(
      Map<String, DataSearcher> dataSearchers, MultiStorageSearchRequest searchRequest,
      ActorRef<BufferSinkActor.Command> bufferActorSinkRef) {
    return Behaviors.setup(ctx -> new DataSourceActor(ctx, dataSearchers, searchRequest, bufferActorSinkRef));
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
    log.debug("The part of the flow was completed");
    sourceIsEmpty.set(true);
    return this;
  }

  private Behavior<Command> onStatusFlow(StatusFlow cmd) {
    cmd.replyTo.tell(StatusReply.success(new StatusResponse(
        sourceIsEmpty.get(), errorCounter.get(), foundByStorageCount.get(), matchedCount.get())));
    return this;
  }

  private Behavior<Command> onCloseFlow(CloseFlow message) {
    log.debug("The flow actor was asked to stop");
    return Behaviors.stopped();
  }

  private void runFlow() {
    log.debug("The flow was asked to run");
    getSource()
        .alsoTo(Sink.foreach(i -> matchedCount.incrementAndGet()))
        .toMat(BufferSinkActor.getSink(getContext().getSelf(), bufferActorSinkRef), Keep.right())
        .withAttributes(ActorAttributes.withSupervisionStrategy(Supervision.getResumingDecider()))
        .run(getContext().getSystem());
    log.debug("Flow was created");
  }

  private Source<SearchItem, NotUsed> getSource() {
    var requestStorageMap = multiStorageSearchRequest.getRequestStorageMap();
    int parallelism = multiStorageSearchRequest.getProcessingSettings().getParallelism();
    if (requestStorageMap.size() == 1) {
      return getSource(dataSearchers.values().iterator().next(),
          multiStorageSearchRequest.getRequestStorageMap().values().iterator().next(), parallelism);
    } else if (requestStorageMap.size() == 2) {
      List<String> storageNames = new ArrayList<>(requestStorageMap.keySet());
      return Source.combine(
          getSource(dataSearchers.get(storageNames.get(0)), requestStorageMap.get(storageNames.get(0)), parallelism),
          getSource(dataSearchers.get(storageNames.get(1)), requestStorageMap.get(storageNames.get(1)), parallelism),
          null,
          Merge::create);
    } else {
      List<String> storageNames = new ArrayList<>(requestStorageMap.keySet());
      List<Source<SearchItem, ?>> source = storageNames.subList(2, storageNames.size()).stream()
          .map(storage -> getSource(dataSearchers.get(storage), requestStorageMap.get(storage), parallelism))
          .collect(Collectors.toList());
      return Source.combine(
          getSource(dataSearchers.get(storageNames.get(0)), requestStorageMap.get(storageNames.get(0)), parallelism),
          getSource(dataSearchers.get(storageNames.get(1)), requestStorageMap.get(storageNames.get(1)), parallelism),
          source,
          Merge::create);
    }
  }

  private Source<SearchItem, NotUsed> getSource(
      DataSearcher dataSearcher, RequestStructure requestStructure, int parallelism) {
    Source<StorageItem, NotUsed> source =
        Source.fromIterator(() -> new Iterator<StorageItem>() {
          Iterator<? extends StorageItem> data = dataSearcher.next().iterator();

          @Override
          public boolean hasNext() {
            if (!data.hasNext()) {
              var nextBatch = dataSearcher.next();
              data = nextBatch.iterator();
            }
            return data.hasNext();
          }

          @Override
          public StorageItem next() {
            foundByStorageCount.incrementAndGet();
            return data.next();
          }
        }).withAttributes(ActorAttributes.withSupervisionStrategy(Supervision.getStoppingDecider()));
    return addFlowStep(source, errorCounter, requestStructure.getResultTransformer(),
        requestStructure.getResultFilter(), parallelism);
  }

  private Source<SearchItem, NotUsed> addFlowStep(
      Source<StorageItem, NotUsed> source, AtomicLong countOfErrors,
      Function<StorageItem, SearchItem> resultTransformer, Predicate<StorageItem> resultFilter, int parallelism) {
    var wrappedStep = wrapStep(resultTransformer, countOfErrors);
    var filterStep = filterStep(resultFilter, countOfErrors);

    return source.via(
        balancer(Flow.of(StorageItem.class)
                .filter(filterStep::test)
                .map(wrappedStep::apply)
                .addAttributes(Attributes.inputBuffer(1, 1)),
            parallelism)
    );
  }

  @SuppressWarnings("unchecked")
  public static Flow<StorageItem, SearchItem, NotUsed> balancer(
      Flow<StorageItem, SearchItem, NotUsed> worker, int workerCount) {
    return Flow.fromGraph(
        GraphDSL.create(
            b -> {
              final UniformFanOutShape<StorageItem, StorageItem> balance =
                  b.add(Balance.create(workerCount, true));
              final UniformFanInShape<SearchItem, SearchItem> merge = b.add(Merge.create(workerCount));

              for (int i = 0; i < workerCount; i++) {
                b.from(balance.out(i)).via(b.add(worker.async())).toInlet(merge.in(i));
              }

              return FlowShape.of(balance.in(), merge.out());
            })).addAttributes(Attributes.inputBuffer(1, 1));
  }

  private Predicate<StorageItem> filterStep(Predicate<StorageItem> filter, AtomicLong countOfErrors) {
    return (t) -> {
      try {
        return filter.test(t);
      } catch (RuntimeException e) {
        log.error("Molecule filter step failed to process data: {}", t, e);
        countOfErrors.incrementAndGet();
        throw e;
      }
    };
  }

  private Function<StorageItem, SearchItem> wrapStep(
      Function<StorageItem, SearchItem> transformation, AtomicLong countOfErrors) {
    return t -> {
      try {
        return transformation.apply(t);
      } catch (RuntimeException e) {
        log.error("Molecule transformation step failed to process data: {}", t, e);
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
