package com.quantori.qdp.core.task.model;

import java.time.LocalDateTime;
import java.util.Map;

public record StreamTaskDetails(String taskId, TaskType type, String user, LocalDateTime created,
                                Map<String, String> details,
                                StreamTaskStatus.Status currentStatus) {
    public enum TaskType {
        Upload, //flow, subtasks:
        UploadInit,
        UploadValidate,
        UploadCreateIndex,
        UploadProperties,
        UploadProcess,
        UploadInfo,

        Merge, //flow, subtasks:
        MergeInit,
        MergeProcess,

        Export,
        BulkEdit,
        BulkDelete,
        BulkUpdate
    }
}
