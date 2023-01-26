package com.quantori.qdp.core.task.model;

import java.time.LocalDateTime;
import java.util.Map;

public record StreamTaskDetails(String taskId, String type, String user, LocalDateTime created,
                                Map<String, String> details,
                                StreamTaskStatus.Status currentStatus) {
}
