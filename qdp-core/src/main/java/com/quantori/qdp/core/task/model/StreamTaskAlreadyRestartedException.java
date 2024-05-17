package com.quantori.qdp.core.task.model;

import java.util.UUID;

public class StreamTaskAlreadyRestartedException extends StreamTaskProcessingException {
    public StreamTaskAlreadyRestartedException(UUID taskId) {
        super(String.format("Task %s was already restarted.", taskId));
    }
}
