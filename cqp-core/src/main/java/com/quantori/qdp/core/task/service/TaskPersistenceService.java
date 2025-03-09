package com.quantori.qdp.core.task.service;

import akka.actor.typed.ActorRef;
import com.quantori.qdp.core.task.actor.StreamTaskActor;
import com.quantori.qdp.core.task.model.ResumableTaskDescription;
import com.quantori.qdp.core.task.model.StreamTaskDescription;
import com.quantori.qdp.core.task.model.StreamTaskResult;
import com.quantori.qdp.core.task.model.StreamTaskStatus;
import com.quantori.qdp.core.task.model.TaskStatus;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

public interface TaskPersistenceService {
  CompletionStage<StreamTaskStatus> resumeTask(UUID taskId, String flowId);

  CompletionStage<StreamTaskStatus> resumeFlow(UUID flowId);

  void persistTask(
      UUID taskId,
      StreamTaskStatus.Status status,
      ResumableTaskDescription task,
      String flowId,
      int parallelism,
      int buffer);

  CompletionStage<StreamTaskStatus> startTaskCommand(
      ActorRef<StreamTaskActor.Command> actorRef,
      StreamTaskDescription streamTaskDescription,
      String flowId,
      int parallelism,
      int buffer);

  boolean taskActorDoesNotExists(UUID taskId) throws ExecutionException, InterruptedException;

  TaskStatus grabSubTaskStatus(UUID taskId);

  CompletionStage<ActorRef<StreamTaskActor.Command>> resumeTaskCommand(UUID taskId);

  void restartInProgressTasks();

  void persistFlowState(
      Consumer<Boolean> onComplete,
      List<StreamTaskDescription> descriptions,
      int currentTaskNumber,
      String currentTaskId,
      List<Float> taskWeights,
      StreamTaskResult lastTaskResult,
      String taskId,
      StreamTaskStatus.Status status,
      String type,
      String user,
      int parallelism,
      int buffer);

  StreamTaskDescription restoreTaskFromStatus(TaskStatus subtaskStatus);

  void updateStatus(String flowId);
}
