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
import com.quantori.qdp.core.source.model.DataSource;
import com.quantori.qdp.core.source.model.PipelineStatistics;
import com.quantori.qdp.core.source.model.TransformationStep;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

//TODO: Move to internal package. This class is not a part of public API.
public class Loader<U, I> {

  private final ActorSystem<?> actorSystem;

  public Loader(ActorSystem<?> actorSystem) {
    this.actorSystem = actorSystem;
  }

  public CompletionStage<PipelineStatistics> loadStorageItems(
      DataSource<U> dataSource, TransformationStep<U, I> transformation, Consumer<I> consumer) {

    final var countOfSuccessfullyProcessed = new AtomicInteger();
    final var countOfErrors = new AtomicInteger();

    Source<U, NotUsed> source = createStreamSource(dataSource, countOfErrors);

    Source<I, NotUsed> transStep =
        addFlowStep(source.zipWithIndex(), transformation, countOfSuccessfullyProcessed, countOfErrors);

    final Sink<I, CompletionStage<Done>> sink = Sink.foreach(m -> {
      try {
        consumer.accept(m);
        countOfSuccessfullyProcessed.incrementAndGet();
      } catch (RuntimeException e) {
        countOfErrors.incrementAndGet();
        throw e;
      }
    });
    return transStep
        .toMat(sink, Keep.right())
        .withAttributes(ActorAttributes.withSupervisionStrategy(decider(transformation)))
        .run(actorSystem)
        .thenApply(done -> new PipelineStatistics(countOfSuccessfullyProcessed.get(), countOfErrors.get()));
  }

  private akka.japi.function.Function<Throwable, Supervision.Directive> decider(
      final TransformationStep<U, I> transformation) {
    return exc -> {
      final Set<Class<? extends Throwable>> errorTypes = transformation.stopOnErrors();
      if (errorTypes == null || errorTypes.isEmpty()) {
        return (Supervision.Directive) Supervision.resume();
      }
      var cause = exc.getCause();
      if (cause instanceof CountableError) {
        cause = cause.getCause();
      }
      if (errorTypes.contains(cause.getClass())) {
        return (Supervision.Directive) Supervision.stop();
      }
      return (Supervision.Directive) Supervision.resume();
    };
  }

  private Source<I, NotUsed> addFlowStep(
      Source<Pair<U, Long>, NotUsed> source,
      TransformationStep<U, I> transformation,
      AtomicInteger countOfSuccessfullyProcessed, AtomicInteger countOfErrors) {
    var wrappedStep = wrapStep(dataItem -> {
      try {
        return transformation.apply(dataItem.first());
      } catch (Exception e) {
        throw new CountableError(dataItem.second(), countOfSuccessfullyProcessed.get(), e);
      }
    }, countOfErrors);

    Source<I, NotUsed> transStep;

    if (transformation.parallelism() <= 1 && transformation.buffer() <= 0) {
      // Async boundary here is to split reading from data source and next step to different threads and add buffer.
      transStep = source.async().map(wrappedStep::apply);
    } else {
      //TODO: add executor.
      transStep = source.mapAsync(transformation.parallelism(),
          i -> CompletableFuture.supplyAsync(() -> wrappedStep.apply(i)));
    }

    if (transformation.buffer() > 0) {
      transStep = transStep.buffer(transformation.buffer(), OverflowStrategy.backpressure());
    }

    if (transformation.throttlingElements() > 0 && transformation.throttlingDuration() != null) {
      transStep = transStep.throttle(transformation.throttlingElements(), transformation.throttlingDuration());
    }
    return transStep;
  }

  private Source<U, NotUsed> createStreamSource(
      DataSource<U> dataSource, AtomicInteger countOfErrors) {
    return Source.fromIterator(() -> {
      try {
        return dataSource.createIterator();
      } catch (Exception e) {
        countOfErrors.incrementAndGet();
        throw e;
      }
    }).withAttributes(ActorAttributes.withSupervisionStrategy(Supervision.getStoppingDecider()));
  }

  private Function<Pair<U, Long>, I> wrapStep(
      TransformationStep<Pair<U, Long>, I> transformation,
      AtomicInteger countOfErrors) {
    return t -> {
      try {
        return transformation.apply(t);
      } catch (Exception e) {
        countOfErrors.incrementAndGet();
        throw e;
      }
    };
  }
}
