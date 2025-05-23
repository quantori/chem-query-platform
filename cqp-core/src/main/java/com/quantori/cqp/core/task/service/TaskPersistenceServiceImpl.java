package com.quantori.cqp.core.task.service;

import static com.quantori.cqp.core.task.actor.StreamTaskActor.taskActorKey;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.quantori.cqp.core.source.SourceRootActor;
import com.quantori.cqp.core.task.TaskServiceActor;
import com.quantori.cqp.core.task.actor.StreamTaskActor;
import com.quantori.cqp.core.task.actor.TaskFlowActor;
import com.quantori.cqp.core.task.dao.TaskStatusDao;
import com.quantori.cqp.core.task.model.FlowDescriptionSerDe;
import com.quantori.cqp.core.task.model.FlowFinalizer;
import com.quantori.cqp.core.task.model.FlowFinalizerSerDe;
import com.quantori.cqp.core.task.model.FlowState;
import com.quantori.cqp.core.task.model.ResumableTaskDescription;
import com.quantori.cqp.core.task.model.StreamTaskAlreadyRestartedException;
import com.quantori.cqp.core.task.model.StreamTaskDescription;
import com.quantori.cqp.core.task.model.StreamTaskProcessingException;
import com.quantori.cqp.core.task.model.StreamTaskResult;
import com.quantori.cqp.core.task.model.StreamTaskStatus;
import com.quantori.cqp.core.task.model.TaskDescriptionSerDe;
import com.quantori.cqp.core.task.model.TaskStatus;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class TaskPersistenceServiceImpl implements TaskPersistenceService {
  private static final Logger logger =
      LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final Duration STALE_THRESHOLD = Duration.ofMinutes(15);
  private static final Duration OUTDATED_THRESHOLD = Duration.ofHours(24);
  private static final Duration RESTART_FLAG_THRESHOLD = Duration.ofMinutes(5);
  private final ObjectMapper objectMapper;
  private final ActorSystem<?> actorSystem;
  private final ActorRef<TaskServiceActor.Command> rootActorRef;
  private final Supplier<StreamTaskService> streamTaskServiceSupplier;
  private final TaskStatusDao taskStatusDao;
  private final Object entityHolder;
  private final boolean schedulingIsEnabled;

  public TaskPersistenceServiceImpl(
      ActorSystem<SourceRootActor.Command> system,
      ActorRef<TaskServiceActor.Command> actorRef,
      Supplier<StreamTaskService> streamTaskServiceSupplier,
      TaskStatusDao taskStatusDao,
      Object entityHolder,
      boolean enableScheduling) {
    this.actorSystem = system;
    this.rootActorRef = actorRef;
    this.streamTaskServiceSupplier = streamTaskServiceSupplier;
    this.taskStatusDao = taskStatusDao;
    this.entityHolder = entityHolder;
    this.objectMapper = new ObjectMapper()
      .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
      .configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

    this.schedulingIsEnabled = enableScheduling;
    if (enableScheduling) {
      actorSystem
          .scheduler()
          .scheduleWithFixedDelay(
              Duration.ofMinutes(1),
              Duration.ofMinutes(1),
              this::restartInProgressTasks,
              actorSystem.executionContext());
    }
  }

  @Override
  public void restartInProgressTasks() {
    Set<String> resumedFlows = new HashSet<>();
    List<TaskStatus> all = taskStatusDao.findAll();

    for (TaskStatus task : all) {
      try {
        if (!isStaleAndUnassigned(task)) {
          checkOutdatedTask(task);
          continue;
        }

        if (StreamTaskStatus.Status.IN_PROGRESS == task.getStatus()) {
          String flowId = task.getFlowId();
          if (flowId == null) {
            resume(resumedFlows, task);
          } else {
            resumedFlows.add(flowId);
          }
        } else {
          handleFinishedOrFailed(task);
        }
      } catch (StreamTaskAlreadyRestartedException e) {
        logger.debug("Task {} was likely restarted elsewhere", task.getTaskId(), e);
      } catch (Exception e) {
        logger.error("Cannot restart task {}", task.getTaskId(), e);
      }
      checkOutdatedTask(task);
    }

    all.stream()
            .filter(t -> t.getFlowId() != null && !resumedFlows.contains(t.getFlowId()))
            .forEach(this::handleFinishedOrFailed);
  }

  private boolean isStaleAndUnassigned(TaskStatus taskStatus) {
    Instant now = Instant.now();
    return taskActorDoesNotExists(taskStatus.getTaskId())
            && Duration.between(taskStatus.getUpdatedDate().toInstant(), now).compareTo(STALE_THRESHOLD) > 0;
  }

  private void handleFinishedOrFailed(TaskStatus taskStatus) {
    String flowId = taskStatus.getFlowId();
    if (flowId == null || taskActorDoesNotExists(UUID.fromString(flowId))) {
      deleteStatusTask(taskStatus.getTaskId());
    } else {
      logger.debug("Skipping delete of child {} because parent {} still has an actor",
              taskStatus.getTaskId(), flowId);
    }
  }

  private void checkOutdatedTask(TaskStatus taskStatus) {
    Instant now = Instant.now();

    boolean inProgOrInit = StreamTaskStatus.Status.IN_PROGRESS == taskStatus.getStatus()
            || StreamTaskStatus.Status.INITIATED == taskStatus.getStatus();
    if (!inProgOrInit) return;

    Instant created = taskStatus.getCreatedDate().toInstant();
    if (Duration.between(created, now).compareTo(OUTDATED_THRESHOLD) > 0) {
      taskStatus.setStatus(StreamTaskStatus.Status.COMPLETED_WITH_ERROR);
      taskStatusDao.save(taskStatus);
      return;
    }

    Instant updated = taskStatus.getUpdatedDate().toInstant();
    if (Duration.between(updated, now).compareTo(RESTART_FLAG_THRESHOLD) > 0
            && taskStatus.getRestartFlag() > 0) {
      taskStatus.setRestartFlag(0);
      taskStatusDao.save(taskStatus);
    }
  }

  private void resume(Set<String> flowIdsForResume, TaskStatus task) {
    if (isFlow(task)) {
      flowIdsForResume.add(task.getTaskId().toString());
      resumeFlow(task.getTaskId());
    } else {
      resumeTask(task.getTaskId(), null);
    }
  }

  private boolean isFlow(TaskStatus task) {
    return FlowDescriptionSerDe.class.getTypeName().equals(task.getDeserializer());
  }

  @Override
  public boolean taskActorDoesNotExists(UUID taskId) {
    try {
      ServiceKey<StreamTaskActor.Command> serviceKey = taskActorKey(taskId.toString());

      CompletionStage<Receptionist.Listing> cf =
          AskPattern.ask(
              actorSystem.receptionist(),
              ref -> Receptionist.find(serviceKey, ref),
              Duration.ofMinutes(1),
              actorSystem.scheduler());

      return cf.toCompletableFuture()
          .thenApply(listing -> listing.getServiceInstances(serviceKey).isEmpty())
          .get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new StreamTaskProcessingException(
          "Cannot check if actor for the task is running: " + taskId);
    } catch (ExecutionException e) {
      throw new StreamTaskProcessingException("Error in a check for the task actor : " + taskId, e);
    }
  }

  @Override
  public CompletionStage<StreamTaskStatus> resumeFlow(UUID flowId) {
    TaskStatus taskStatus = markForTaskExecution(flowId);

    if (Objects.nonNull(taskStatus)) {
      try {
        var state = objectMapper.readValue(taskStatus.getState(), FlowState.class);
        List<StreamTaskDescription> tasks = restoreFlowDescription(state);
        Consumer<Boolean> onComplete = restoreOnComplete(state);

        StreamTaskResult lastResult = getStreamTaskResult(state);

        CompletionStage<ActorRef<StreamTaskActor.Command>> stage =
            AskPattern.askWithStatus(
                rootActorRef,
                repl ->
                    new TaskServiceActor.ResumeFlow(
                        repl,
                        streamTaskServiceSupplier.get(),
                        this,
                        flowId.toString(),
                        taskStatus.getType()),
                Duration.ofMinutes(1),
                actorSystem.scheduler());
        return stage.thenCompose(
            actorRef ->
                resumeFlowCommand(actorRef, state, tasks, lastResult, onComplete, taskStatus));

      } catch (IOException e) {
        throw new StreamTaskProcessingException("Cannot read serialized flow data: " + flowId, e);
      }
    } else {
      throw new StreamTaskProcessingException("Cannot find a Flow status: " + flowId);
    }
  }

  private StreamTaskResult getStreamTaskResult(FlowState state) {
    if (StringUtils.isBlank(state.getLastTaskResultType())
        || StringUtils.isBlank(state.getLastTaskResult())) {
      return null;
    }
    try {
      return (StreamTaskResult)
          objectMapper.readValue(
              state.getLastTaskResult(), Class.forName(state.getLastTaskResultType()));
    } catch (ClassNotFoundException | JsonProcessingException e) {
      return null;
    }
  }

  private List<StreamTaskDescription> restoreFlowDescription(FlowState state) {
    var it = state.getTasksStates().iterator();
    return state.getTaskFactories().stream()
        .map(this::getDeserializer)
        .map(e -> e.deserialize(it.next()))
        .toList();
  }

  private Consumer<Boolean> restoreOnComplete(FlowState state) {
    if (StringUtils.isBlank(state.getFinalizerFactory())) {
      return null;
    }

    try {
      Class<?> clazz = Class.forName(state.getFinalizerFactory());
      Constructor<?> constructor = clazz.getConstructor();
      FlowFinalizerSerDe instance = (FlowFinalizerSerDe) constructor.newInstance();
      instance.setRequiredEntities(entityHolder);
      return instance.deserialize(state.getFinalizerData());
    } catch (InstantiationException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException
        | ClassNotFoundException e) {
      throw new StreamTaskProcessingException(
          "Cannot restore onComplete from state : " + state.getFinalizerData(), e);
    }
  }

  @Override
  public CompletionStage<StreamTaskStatus> resumeTask(UUID taskId, String flowId) {
    if (!taskActorDoesNotExists(taskId)) {
      throw new StreamTaskProcessingException("The task is still running by actor: " + taskId);
    }
    TaskStatus taskStatus = markForTaskExecution(taskId);
    if (Objects.nonNull(taskStatus)) {
      StreamTaskDescription task = restoreTaskDescription(taskStatus);

      CompletionStage<ActorRef<StreamTaskActor.Command>> stage = resumeTaskCommand(taskId);

      return stage.thenCompose(
          actorRef ->
              startTaskCommand(
                  actorRef, task, flowId, taskStatus.getParallelism(), taskStatus.getBuffer()));
    } else {
      throw new StreamTaskProcessingException("Cannot find a task status: " + taskId);
    }
  }

  @Override
  public CompletionStage<ActorRef<StreamTaskActor.Command>> resumeTaskCommand(UUID taskId) {
    return AskPattern.askWithStatus(
        rootActorRef,
        repl -> new TaskServiceActor.ResumeTask(repl, this, taskId.toString()),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  @Override
  public void persistTask(
      UUID taskId,
      StreamTaskStatus.Status status,
      ResumableTaskDescription rTask,
      String flowId,
      int parallelism,
      int buffer) {
    TaskStatus taskstatus =
        new TaskStatus()
            .setTaskId(taskId)
            .setDeserializer(rTask.getSerDe().getClass().getTypeName())
            .setState(rTask.getSerDe().serialize(rTask.getState()))
            .setRestartFlag(0)
            .setParallelism(parallelism)
            .setBuffer(buffer)
            .setFlowId(flowId)
            .setType(rTask.getType())
            .setUser(rTask.getUser())
            .setStatus(status);

    taskStatusDao.save(taskstatus);
  }

  private void deleteStatusTask(UUID taskId) {
    taskStatusDao.findAllById(List.of(taskId)).forEach(taskStatusDao::delete);
  }

  @Override
  public CompletionStage<StreamTaskStatus> startTaskCommand(
      ActorRef<StreamTaskActor.Command> actorRef,
      StreamTaskDescription streamTaskDescription,
      String flowId,
      int parallelism,
      int buffer) {
    return AskPattern.askWithStatus(
        actorRef,
        replyTo ->
            new StreamTaskActor.StartTask(
                replyTo, streamTaskDescription, flowId, parallelism, buffer),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  private CompletionStage<StreamTaskStatus> resumeFlowCommand(
      ActorRef<StreamTaskActor.Command> actorRef,
      FlowState state,
      List<StreamTaskDescription> streamTaskDescriptions,
      StreamTaskResult lastResult,
      Consumer<Boolean> onComplete,
      TaskStatus taskStatus) {
    return AskPattern.askWithStatus(
        actorRef,
        replyTo ->
            new TaskFlowActor.ResumeFlow(
                replyTo,
                state.getCurrentTaskNumber(),
                streamTaskDescriptions,
                state.getTaskWeights(),
                onComplete,
                lastResult,
                state.getCurrentTaskId(),
                taskStatus.getUser(),
                taskStatus.getParallelism(),
                taskStatus.getBuffer()),
        Duration.ofMinutes(1),
        actorSystem.scheduler());
  }

  private StreamTaskDescription restoreTaskDescription(TaskStatus taskStatus) {
    TaskDescriptionSerDe deserializer = getDeserializer(taskStatus.getDeserializer());
    return deserializer.deserialize(taskStatus.getState());
  }

  @Override
  public TaskStatus grabSubTaskStatus(UUID taskId) {
    return taskStatusDao.grabSubTaskStatus(taskId);
  }

  private TaskStatus markForTaskExecution(UUID taskId) {
    return taskStatusDao.markForTaskExecution(taskId);
  }

  private TaskDescriptionSerDe getDeserializer(String deserializerClass) {
    try {
      var clazz = Class.forName(deserializerClass);
      Constructor<?> constructor = clazz.getConstructor();
      TaskDescriptionSerDe serDe = (TaskDescriptionSerDe) constructor.newInstance();
      serDe.setRequiredEntities(entityHolder);
      return serDe;
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | InvocationTargetException
        | InstantiationException
        | IllegalAccessException e) {
      logger.error("Cannot instantiate deserializer class {}", deserializerClass, e);
    }
    throw new StreamTaskProcessingException(
        "Cannot restart a task with deserializerClass : " + deserializerClass);
  }

  @Override
  public void persistFlowState(
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
      int buffer) {
    if (isPersistent(onComplete, descriptions)) {
      var states =
          descriptions.stream()
              .map(ResumableTaskDescription.class::cast)
              .map(rTask -> rTask.getSerDe().serialize(rTask.getState()))
              .toList();

      var factories =
          descriptions.stream()
              .map(ResumableTaskDescription.class::cast)
              .map(e -> e.getSerDe().getClass().getTypeName())
              .toList();

      FlowFinalizer finalizer = (FlowFinalizer) onComplete;

      try {
        FlowState state =
            new FlowState(
                currentTaskNumber,
                states,
                factories,
                finalizer == null ? null : finalizer.getSerializer().getClass().getTypeName(),
                finalizer == null
                    ? null
                    : finalizer.getSerializer().serialize(finalizer.getParameters()),
                taskWeights,
                lastTaskResult == null ? null : objectMapper.writeValueAsString(lastTaskResult),
                lastTaskResult == null ? null : lastTaskResult.getClass().getTypeName(),
                currentTaskId);
        var serialized = objectMapper.writeValueAsString(state);

        TaskStatus taskstatus =
            new TaskStatus()
                .setTaskId(UUID.fromString(taskId))
                .setDeserializer(FlowDescriptionSerDe.class.getTypeName())
                .setState(serialized)
                .setRestartFlag(0)
                .setFlowId(null)
                .setUser(user)
                .setType(type)
                .setParallelism(parallelism)
                .setBuffer(buffer)
                .setStatus(status);

        taskStatusDao.save(taskstatus);
      } catch (IOException e) {
        logger.error("Cannot persist the flow state {} for user {} ", taskId, user, e);
      }
    }
  }

  @Override
  public StreamTaskDescription restoreTaskFromStatus(TaskStatus subtaskStatus) {
    TaskDescriptionSerDe deserializer = getDeserializer(subtaskStatus.getDeserializer());
    return deserializer.deserialize(subtaskStatus.getState());
  }

  @Override
  public void updateStatus(String taskId) {
    taskStatusDao
        .findById(UUID.fromString(taskId))
        .ifPresent(
            taskStatus -> {
              logger.debug("Periodical task update, id: {}", taskId);
              taskStatus.setUpdatedDate(Date.from(Instant.now()));
              taskStatusDao.save(taskStatus);
            });
  }

  private boolean isPersistent(
      Consumer<Boolean> onComplete, List<StreamTaskDescription> descriptions) {
    if (Objects.nonNull(onComplete) && !(onComplete instanceof FlowFinalizer)) {
      return false;
    }
    return descriptions.stream().filter(ResumableTaskDescription.class::isInstance).count()
        == descriptions.size();
  }
}
