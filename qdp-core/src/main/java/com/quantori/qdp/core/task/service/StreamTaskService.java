package com.quantori.qdp.core.task.service;

import com.quantori.qdp.core.task.model.StreamTaskDescription;
import com.quantori.qdp.core.task.model.StreamTaskDetails;
import com.quantori.qdp.core.task.model.StreamTaskResult;
import com.quantori.qdp.core.task.model.StreamTaskStatus;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

public interface StreamTaskService {

    CompletionStage<StreamTaskStatus> getTaskStatus(String taskId, String user);

    CompletionStage<StreamTaskResult> getTaskResult(String taskId, String user);

    CompletionStage<StreamTaskDetails> getTaskDetails(String taskId, String user);

    CompletionStage<StreamTaskStatus> processTask(StreamTaskDescription task, String flowId);

    CompletionStage<StreamTaskStatus> processTask(StreamTaskDescription task, String flowId, int parallelism, int buffer);

    CompletionStage<StreamTaskStatus> processTaskFlowAsTask(List<StreamTaskDescription> tasks, String type,
                                                            Consumer<Boolean> onComplete, String user);

    CompletionStage<StreamTaskStatus> processTaskFlowAsTask(List<StreamTaskDescription> tasks, String type,
                                                            Consumer<Boolean> onComplete, String user, int parallelism,
                                                            int buffer);

    void closeTask(String taskId, String user);

    CompletionStage<StreamTaskStatus> cancelTask(String taskId, String user);

    CompletionStage<List<StreamTaskDetails>> getUserTaskStatus(String name);
}
