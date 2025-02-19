package com.quantori.qdp.core.source;

import akka.Done;
import akka.NotUsed;
import akka.actor.typed.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorAttributes;
import akka.stream.OverflowStrategy;
import akka.stream.Supervision;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.quantori.qdp.core.model.DataSource;
import com.quantori.qdp.core.model.DataUploadItem;
import com.quantori.qdp.core.model.PipelineStatistics;
import com.quantori.qdp.core.model.StorageUploadItem;
import com.quantori.qdp.core.model.TransformationStep;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
class Loader<D extends DataUploadItem, U extends StorageUploadItem> {

  private final ActorSystem<?> actorSystem;

  Loader(ActorSystem<?> actorSystem) {
    this.actorSystem = actorSystem;
  }

  CompletionStage<PipelineStatistics> loadStorageItems(
      DataSource<D> dataSource, TransformationStep<D, U> transformation, Consumer<U> consumer) {

    final var countOfSuccessfullyProcessed = new AtomicInteger();
    final var countOfErrors = new AtomicInteger();

    Source<D, NotUsed> source = createStreamSource(dataSource, countOfErrors);

    Source<U, NotUsed> transStep =
        addFlowStep(
            source.zipWithIndex(), transformation, countOfSuccessfullyProcessed, countOfErrors);

    final Sink<U, CompletionStage<Done>> sink =
        Sink.foreach(
            m -> {
              try {
                consumer.accept(m);
                countOfSuccessfullyProcessed.incrementAndGet();
              } catch (RuntimeException e) {
                log.error(e.getMessage(), e);
                countOfErrors.incrementAndGet();
                throw e;
              }
            });
    return transStep
        .toMat(sink, Keep.right())
        .withAttributes(ActorAttributes.withSupervisionStrategy(decider(transformation)))
        .run(actorSystem)
        .thenApply(
            done ->
                new PipelineStatistics(countOfSuccessfullyProcessed.get(), countOfErrors.get()));
  }

  private akka.japi.function.Function<Throwable, Supervision.Directive> decider(
      final TransformationStep<D, U> transformation) {
    return exc -> {
      final Set<Class<? extends Throwable>> errorTypes = transformation.stopOnErrors();
      if (errorTypes == null || errorTypes.isEmpty()) {
        return (Supervision.Directive) Supervision.resume();
      }
      var cause = exc.getCause();
      if (cause instanceof CountableError) {
        cause = cause.getCause();
      }
      if (cause != null && errorTypes.contains(cause.getClass())) {
        log.error(exc.getMessage(), exc);
        return (Supervision.Directive) Supervision.stop();
      }
      log.warn(exc.getMessage(), exc);
      return (Supervision.Directive) Supervision.resume();
    };
  }

  private Source<U, NotUsed> addFlowStep(
      Source<Pair<D, Long>, NotUsed> source,
      TransformationStep<D, U> transformation,
      AtomicInteger countOfSuccessfullyProcessed,
      AtomicInteger countOfErrors) {
    var wrappedStep =
        wrapStep(
            dataItem -> {
              try {
                return transformation.apply(dataItem.first());
              } catch (Exception e) {
                log.error(e.getMessage(), e);
                throw new CountableError(dataItem.second(), countOfSuccessfullyProcessed.get(), e);
              }
            },
            countOfErrors);

    Source<U, NotUsed> transStep;

    if (transformation.parallelism() <= 1 && transformation.buffer() <= 0) {
      // Async boundary here is to split reading from data source and next step to different threads
      // and add buffer.
      transStep = source.async().map(wrappedStep::apply);
    } else {
      // TODO: add executor.
      transStep =
          source.mapAsync(
              transformation.parallelism(),
              i -> CompletableFuture.supplyAsync(() -> wrappedStep.apply(i)));
    }

    if (transformation.buffer() > 0) {
      transStep = transStep.buffer(transformation.buffer(), OverflowStrategy.backpressure());
    }

    if (transformation.throttlingElements() > 0 && transformation.throttlingDuration() != null) {
      transStep =
          transStep.throttle(
              transformation.throttlingElements(), transformation.throttlingDuration());
    }
    return transStep;
  }

  private Source<D, NotUsed> createStreamSource(
      DataSource<D> dataSource, AtomicInteger countOfErrors) {
    return Source.fromIterator(
            () -> {
              try {
                return dataSource.createIterator();
              } catch (Exception e) {
                log.error(e.getMessage(), e);
                countOfErrors.incrementAndGet();
                throw e;
              }
            })
        .withAttributes(ActorAttributes.withSupervisionStrategy(Supervision.getStoppingDecider()));
  }

  private Function<Pair<D, Long>, U> wrapStep(
      TransformationStep<Pair<D, Long>, U> transformation, AtomicInteger countOfErrors) {
    return t -> {
      try {
        return transformation.apply(t);
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        countOfErrors.incrementAndGet();
        throw e;
      }
    };
  }
}
