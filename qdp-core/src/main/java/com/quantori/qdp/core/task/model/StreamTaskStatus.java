package com.quantori.qdp.core.task.model;

import java.util.ArrayList;
import java.util.List;

public record StreamTaskStatus(String taskId, Status status, String stage, double percent, double stagePercent, List<String> messages) {
    public enum Status { INITIATED, IN_PROGRESS, COMPLETED, COMPLETED_WITH_ERROR }

    public StreamTaskStatus(String taskId, Status status, float percent) {
        this(taskId,  status,  "", percent, percent, new ArrayList<>());
    }

    public StreamTaskStatus(String taskId, Status status, String stage, double percent, double stagePercent) {
        this(taskId,  status,  stage, percent, stagePercent, new ArrayList<>());
    }
}
