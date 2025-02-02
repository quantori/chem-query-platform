package com.quantori.qdp.core.model;

import java.time.Duration;
import java.util.Set;
import javax.validation.constraints.NotNull;

/**
 * Apply some transformation operations. Transformation should not return null, it will break processing, instead throw
 * and error. Also the step might filter out elements with help of the test method.
 */
@FunctionalInterface
public interface TransformationStep<T, R> {
    int DEFAULT_PARALLELISM = 1;
    int DEFAULT_BUFFER = 0;
    int DEFAULT_THROTTLING_ELEMENTS = 0;
    Duration DEFAULT_THROTTLING_DURATION = Duration.ofSeconds(1);

    @NotNull
    R apply(T dataItem);

    default int parallelism() {
        return DEFAULT_PARALLELISM;
    }

    default int buffer() {
        return DEFAULT_BUFFER;
    }

    default int throttlingElements() {
        return DEFAULT_THROTTLING_ELEMENTS;
    }

    default Duration throttlingDuration() {
        return DEFAULT_THROTTLING_DURATION;
    }

    default Set<Class<? extends Throwable>> stopOnErrors() {
        return Set.of();
    }
}
