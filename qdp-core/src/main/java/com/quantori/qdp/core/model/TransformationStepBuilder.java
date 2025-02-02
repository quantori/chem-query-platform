package com.quantori.qdp.core.model;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import javax.validation.constraints.NotNull;

public class TransformationStepBuilder<I, O> {
    private final Function<I, O> step;
    private int parallelism = TransformationStep.DEFAULT_PARALLELISM;
    private int buffer = TransformationStep.DEFAULT_BUFFER;
    private int throttlingElements = TransformationStep.DEFAULT_THROTTLING_ELEMENTS;
    private Duration throttlingDuration = TransformationStep.DEFAULT_THROTTLING_DURATION;
    private Set<Class<? extends Throwable>> stopOnErrors = Set.of();

    private TransformationStepBuilder(Function<I, O> step) {
        this.step = step;
    }

    public static <I, O> TransformationStepBuilder<I, O> builder(Function<I, O> f) {
        return new TransformationStepBuilder<>(f);
    }

    public TransformationStepBuilder<I, O> withParallelism(int parallelism) {
        this.parallelism = parallelism;
        return this;
    }

    public TransformationStepBuilder<I, O> withBuffer(int buffer) {
        this.buffer = buffer;
        return this;
    }

    public TransformationStepBuilder<I, O> withThrottling(int throttlingElements, Duration throttlingDuration) {
        this.throttlingElements = throttlingElements;
        this.throttlingDuration = throttlingDuration;
        return this;
    }

    @SafeVarargs
    public final TransformationStepBuilder<I, O> withStopOnErrors(final Class<? extends Throwable>... throwables) {
        if (throwables != null && throwables.length > 0) {
            this.stopOnErrors = new HashSet<>(Arrays.asList(throwables));
        }
        return this;
    }

    public TransformationStep<I, O> build() {
        final var that = this;

        return new TransformationStep<>() {
            @Override
            public @NotNull O apply(I dataItem) {
                return step.apply(dataItem);
            }

            @Override
            public int parallelism() {
                return that.parallelism;
            }

            @Override
            public int buffer() {
                return that.buffer;
            }

            @Override
            public int throttlingElements() {
                return that.throttlingElements;
            }

            @Override
            public Duration throttlingDuration() {
                return that.throttlingDuration;
            }

            @Override
            public Set<Class<? extends Throwable>> stopOnErrors() {
                return that.stopOnErrors;
            }
        };
    }
}
