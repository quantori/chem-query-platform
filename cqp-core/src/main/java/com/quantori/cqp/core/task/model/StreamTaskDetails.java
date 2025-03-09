package com.quantori.cqp.core.task.model;

import java.time.LocalDateTime;
import java.util.Map;

public record StreamTaskDetails(
    String taskId,
    String type,
    String stage,
    String user,
    LocalDateTime created,
    Map<String, String> details,
    StreamTaskStatus.Status currentStatus) {}
