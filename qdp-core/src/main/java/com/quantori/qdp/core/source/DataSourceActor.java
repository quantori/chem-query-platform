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
import com.quantori.qdp.api.model.core.ErrorType;
import com.quantori.qdp.api.model.core.SearchError;
import com.quantori.qdp.api.model.core.SearchItem;
import com.quantori.qdp.api.model.core.SearchRequest;
import com.quantori.qdp.api.model.core.StorageItem;
import com.quantori.qdp.api.service.SearchIterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class DataSourceActor<S extends SearchItem, I extends StorageItem>
    extends AbstractBehavior<DataSourceActor.Command> {
  private final SearchRequest<S, I> searchRequest;
  private final Map<String, List<SearchIterator<I>>> searchIterators;
  private final Collection<SearchError> errors = new ConcurrentLinkedQueue<>();
  private final AtomicLong foundByStorageCount = new AtomicLong(0);
  private final AtomicLong matchedCount = new AtomicLong(0);
  private final AtomicBoolean sourceFetchComplete = new AtomicBoolean(false);
  private final ActorRef<BufferSinkActor.Command> bufferActorSinkRef;

  private DataSourceActor(ActorContext<Command> context, Map<String, List<SearchIterator<I>>> searchIterators,
                          SearchRequest<S, I> searchRequest, ActorRef<BufferSinkActor.Command> bufferActorSinkRef) {
    super(context);
    this.searchIterators = searchIterators;
    this.searchRequest = searchRequest;
    this.bufferActorSinkRef = bufferActorSinkRef;
    runFlow();
  }

  static <S extends SearchItem, I extends StorageItem> Behavior<Command> create(
      Map<String, List<SearchIterator<I>>> searchIterators, SearchRequest<S, I> searchRequest,
      ActorRef<BufferSinkActor.Command> bufferActorSinkRef) {
    return Behaviors.setup(ctx -> new DataSourceActor<>(ctx, searchIterators,
        searchRequest, bufferActorSinkRef));
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
    sourceFetchComplete.set(true);
    return this;
  }

  private Behavior<Command> onStatusFlow(StatusFlow cmd) {
    cmd.replyTo.tell(StatusReply.success(new StatusResponse(
        sourceFetchComplete.get(), new ArrayList<>(errors), foundByStorageCount.get(), matchedCount.get())));
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

  @SuppressWarnings("unchecked")
  static <S, I> Flow<I, S, NotUsed> balancer(Flow<I, S, NotUsed> worker, int workerCount) {
    return Flow.fromGraph(GraphDSL.create(b -> {
      UniformFanOutShape<I, I> balance = b.add(Balance.create(workerCount, true));
      UniformFanInShape<S, S> merge = b.add(Merge.create(workerCount));

      for (int i = 0; i < workerCount; i++) {
        b.from(balance.out(i)).via(b.add(worker.async())).toInlet(merge.in(i));
      }

      return FlowShape.of(balance.in(), merge.out());
    })).addAttributes(Attributes.inputBuffer(1, 1));
  }

  private Source<S, NotUsed> getSource() {
    int parallelism = searchRequest.getParallelism();
    List<SearchIterator<I>> dataSearcherList = searchIterators.values().stream().flatMap(Collection::stream).toList();
    if (dataSearcherList.size() == 1) {
      return getSource(dataSearcherList.get(0), parallelism,
          searchRequest.getResultFilter(), searchRequest.getResultTransformer());
    } else if (dataSearcherList.size() == 2) {
      return Source.combine(getSource(dataSearcherList.get(0), parallelism,
              searchRequest.getResultFilter(), searchRequest.getResultTransformer()),
          getSource(dataSearcherList.get(1), parallelism,
              searchRequest.getResultFilter(), searchRequest.getResultTransformer()),
          null,
          Merge::create);
    } else {
      List<Source<S, ?>> remainIterators = dataSearcherList.subList(2, dataSearcherList.size()).stream()
          .map(dataSearcher -> getSource(dataSearcher, parallelism,
              searchRequest.getResultFilter(), searchRequest.getResultTransformer()))
          .collect(Collectors.toList());
      return Source.combine(getSource(dataSearcherList.get(0), parallelism,
              searchRequest.getResultFilter(), searchRequest.getResultTransformer()),
          getSource(dataSearcherList.get(1), parallelism,
              searchRequest.getResultFilter(), searchRequest.getResultTransformer()),
          remainIterators, Merge::create);
    }
  }

  private Source<S, NotUsed> getSource(SearchIterator<I> searchIterator, int parallelism,
                                       Predicate<I> resultFilter, Function<I, S> resultTransformer) {
    Source<I, NotUsed> source = Source.unfoldResource(() -> searchIterator, si -> {
          if (sourceFetchComplete.get()) {
            return Optional.empty();
          }
          List<I> result;
          try {
            result = si.next();
          } catch (Exception e) {
            log.error(e.getMessage(), e);
            errors.add(new SearchError(ErrorType.STORAGE, si.getStorageName(), si.getLibraryIds(), e.getMessage()));
            return Optional.empty();
          }
          foundByStorageCount.addAndGet(result.size());
          if (!result.isEmpty()) {
            return Optional.of(result);
          } else {
            return Optional.empty();
          }
        }, AutoCloseable::close).mapConcat(list -> list)
        .withAttributes(ActorAttributes.withSupervisionStrategy(Supervision.getStoppingDecider()));
    return addFlowStep(searchIterator.getStorageName(), searchIterator.getLibraryIds(), parallelism, source,
        resultFilter, resultTransformer);
  }

  private Source<S, NotUsed> addFlowStep(
      String storageName, List<String> libraryIds, int parallelism,
      Source<I, NotUsed> source, Predicate<I> resultFilter, Function<I, S> resultTransformer) {
    var filterStep = filterStep(resultFilter, storageName, libraryIds);
    var transformStep = transformStep(resultTransformer, storageName, libraryIds);
    Class<I> flowClass = null;
    Flow<I, S, NotUsed> worker = Flow.of(flowClass)
        .filter(filterStep::test)
        .map(transformStep::apply)
        .addAttributes(Attributes.inputBuffer(1, 1));
    return source.via(balancer(worker, parallelism));
  }

  private Predicate<I> filterStep(Predicate<I> filter, String storageName, List<String> libraryIds) {
    return item -> {
      try {
        return filter.test(item);
      } catch (RuntimeException e) {
        log.error("Filter step failed to process data: {}", item, e);
        errors.add(new SearchError(ErrorType.FILTER, storageName, libraryIds, "Molecule filter step failed"));
        throw e;
      }
    };
  }

  private Function<I, S> transformStep(Function<I, S> transformation, String storageName, List<String> libraryIds) {
    return item -> {
      try {
        return transformation.apply(item);
      } catch (RuntimeException e) {
        log.error("Transformation step failed to process data: {}", item, e);
        errors.add(
            new SearchError(ErrorType.TRANSFORMER, storageName, libraryIds, "Molecule transformation step failed"));
        throw e;
      }
    };
  }

  interface Command {
  }

  static class CompletedFlow implements Command {
  }

  @Value
  static class StatusFlow implements Command {
    ActorRef<StatusReply<StatusResponse>> replyTo;
  }


  @Value
  static class StatusResponse implements Command {
    boolean completed;
    List<SearchError> errors;
    long foundByStorageCount;
    long matchedCount;
  }

  static class CloseFlow implements Command {
  }
}
