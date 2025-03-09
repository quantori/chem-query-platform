package com.quantori.cqp.core.task.actor;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.pattern.StatusReply;
import com.quantori.cqp.core.task.model.DataProvider;
import com.quantori.cqp.core.task.model.DescriptionState;
import com.quantori.cqp.core.task.model.ResultAggregator;
import com.quantori.cqp.core.task.model.ResumableTaskDescription;
import com.quantori.cqp.core.task.model.StreamTaskDescription;
import com.quantori.cqp.core.task.model.StreamTaskDetails;
import com.quantori.cqp.core.task.model.StreamTaskProcessingException;
import com.quantori.cqp.core.task.model.StreamTaskResult;
import com.quantori.cqp.core.task.model.StreamTaskStatus;
import com.quantori.cqp.core.task.model.TaskStatus;
import com.quantori.cqp.core.task.service.StreamTaskService;
import com.quantori.cqp.core.task.service.TaskPersistenceService;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskFlowActor extends StreamTaskActor {
  private static final Logger logger = LoggerFactory.getLogger(TaskFlowActor.class);
  private final Map<Integer, Boolean> statuses = new LinkedHashMap<>();
  private final Map<Integer, String> tasksIds = new HashMap<>();
  private final StreamTaskService service;
  private List<StreamTaskDescription> descriptions;
  private Consumer<Boolean> onComplete;
  private List<Float> taskWeights;
  private int currentTaskNumber;
  private StreamTaskResult lastTaskResult;
  private String user;

  private TaskFlowActor(
      ActorContext<Command> context,
      TimerScheduler<Command> timerScheduler,
      StreamTaskService service,
      TaskPersistenceService taskPersistenceService,
      String type,
      ActorRef<StatusReply<ActorRef<StreamTaskActor.Command>>> replyTo) {
    super(context, timerScheduler, taskPersistenceService, replyTo);
    this.service = service;
    this.type = type;
  }

  private TaskFlowActor(
      ActorContext<Command> context,
      TimerScheduler<Command> timerScheduler,
      StreamTaskService service,
      TaskPersistenceService taskPersistenceService,
      String flowId,
      String type,
      ActorRef<StatusReply<ActorRef<StreamTaskActor.Command>>> replyTo) {
    super(context, timerScheduler, taskPersistenceService, flowId, replyTo);
    this.service = service;
    this.type = type;
  }

  public static Behavior<Command> create(
      StreamTaskService service,
      TaskPersistenceService taskPersistenceService,
      String type,
      ActorRef<StatusReply<ActorRef<StreamTaskActor.Command>>> replyTo) {
    return Behaviors.setup(
        ctx ->
            Behaviors.withTimers(
                timer ->
                    new TaskFlowActor(ctx, timer, service, taskPersistenceService, type, replyTo)));
  }

  public static Behavior<Command> create(
      StreamTaskService service,
      TaskPersistenceService taskPersistenceService,
      String flowId,
      String type,
      ActorRef<StatusReply<ActorRef<StreamTaskActor.Command>>> replyTo) {
    return Behaviors.setup(
        ctx ->
            Behaviors.withTimers(
                timer ->
                    new TaskFlowActor(
                        ctx, timer, service, taskPersistenceService, flowId, type, replyTo)));
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(StreamTaskActor.StartTask.class, this::onStartFlow)
        .onMessage(StreamTaskActor.GetStatus.class, this::onStatus)
        .onMessage(StartFlow.class, this::onStartFlow)
        .onMessage(ResumeFlow.class, this::onResumeFlow)
        .onMessage(AdjustPercent.class, this::onAdjustPercent)
        .onMessage(SubTaskComplete.class, this::onSubTaskComplete)
        .onMessage(StreamTaskActor.GetTaskResult.class, this::onGetFlowResult)
        .onMessage(StreamTaskActor.GetTaskDetails.class, this::onGetFlowDetails)
        .onMessage(StreamTaskActor.Timeout.class, this::onTimeoutSubTasks)
        .onMessage(StreamTaskActor.Close.class, this::onCloseSubTasks)
        .onMessage(StreamTaskActor.Cancel.class, this::onCancel)
        .onMessage(StreamTaskActor.UpdateStatus.class, this::onUpdateStatus)
        .build();
  }

  private Behavior<Command> onAdjustPercent(AdjustPercent cmd) {
    logger.trace(
        "Flow percent {} was updated  {}, from task {}", taskId, cmd.percent, cmd.taskNumber);
    var previouseSteps = 0f;
    if (cmd.taskNumber() > 0) {
      previouseSteps =
          (float)
              taskWeights.stream().limit(cmd.taskNumber()).mapToDouble(Float::doubleValue).sum();
    }
    stagePercent = cmd.percent();
    percent = previouseSteps + stagePercent * taskWeights.get(cmd.taskNumber);
    return Behaviors.withTimers(
        timer -> {
          timer.startSingleTimer(timerId, new Timeout(), timeout);
          return this;
        });
  }

  private Behavior<Command> onGetFlowResult(StreamTaskActor.GetTaskResult cmd) {
    if (Objects.nonNull(user) && !cmd.user().equals(user)) {
      logger.warn("Flow result access violation for user: {}", cmd.user());
      cmd.replyTo()
          .tell(
              StatusReply.error(
                  new StreamTaskProcessingException(
                      "Flow result access violation for user: " + cmd.user())));
      return this;
    }
    switch (status) {
      case COMPLETED -> cmd.replyTo().tell(StatusReply.success(lastTaskResult));
      case COMPLETED_WITH_ERROR ->
          cmd.replyTo()
              .tell(StatusReply.error(new StreamTaskProcessingException("The flow was failed")));
      default ->
          cmd.replyTo()
              .tell(
                  StatusReply.error(
                      new StreamTaskProcessingException("Flow is not completed yet")));
    }
    return Behaviors.withTimers(
        timer -> {
          timer.startSingleTimer(timerId, new Timeout(), timeout);
          return this;
        });
  }

  private Behavior<StreamTaskActor.Command> onGetFlowDetails(GetTaskDetails getTaskDetails) {
    logger.trace("Result request: {}", getTaskDetails);
    if (Objects.nonNull(user) && !getTaskDetails.user().equals(user)) {
      logger.warn("Flow details access violation for user: {}", getTaskDetails.user());
      getTaskDetails
          .replyTo()
          .tell(
              StatusReply.error(
                  new StreamTaskProcessingException(
                      "Flow details access violation for user: " + getTaskDetails.user())));
      return this;
    }
    var details = new HashMap<String, String>();
    descriptions.forEach(d -> details.putAll(d.getDetailsMapProvider().get()));
    getTaskDetails
        .replyTo()
        .tell(
            StatusReply.success(
                new StreamTaskDetails(taskId, type, stage, user, created, details, status)));

    return Behaviors.withTimers(
        timer -> {
          timer.startSingleTimer(timerId, new Timeout(), timeout);
          return this;
        });
  }

  private Behavior<Command> onTimeoutSubTasks(Command cmd) {
    logger.trace("Flow was closed by timeout {}", taskId);
    tasksIds
        .keySet()
        .forEach(
            num -> {
              try {
                closeTaskByNumber(num);
              } catch (RuntimeException e) {
                logger.warn(
                    "Cannot close a sub task {} (task id: {}) in the flow by timeout.",
                    num,
                    tasksIds.get(num),
                    e);
              }
            });
    return Behaviors.stopped();
  }

  private Behavior<Command> onCloseSubTasks(Close cmd) {
    if (user.equals(cmd.user())) {
      logger.trace("Flow was closed {}", taskId);
      tasksIds
          .keySet()
          .forEach(
              num -> {
                try {
                  closeTaskByNumber(num);
                } catch (RuntimeException e) {
                  logger.warn(
                      "Cannot close a sub task {} (task id: {}) in the flow.",
                      num,
                      tasksIds.get(num),
                      e);
                }
              });
      return Behaviors.stopped();
    } else {
      logger.warn("No access for user {} into the flow {}", user, taskId);
      return this;
    }
  }

  private Behavior<Command> onCancel(Cancel cmd) {
    if (Objects.nonNull(user) && !cmd.user().equals(user)) {
      logger.warn("Flow cancel access violation for user: {}", cmd.user());
      cmd.replyTo()
          .tell(
              StatusReply.error(
                  new StreamTaskProcessingException(
                      "Flow cancellation operation access violation for user: " + cmd.user())));
      return this;
    }
    if (!status.equals(StreamTaskStatus.Status.COMPLETED)
        && !status.equals(StreamTaskStatus.Status.COMPLETED_WITH_ERROR)) {
      logger.trace("Flow was canceled {}", taskId);
      this.status = StreamTaskStatus.Status.COMPLETED_WITH_ERROR;
      tasksIds.keySet().stream()
          .sorted(Comparator.reverseOrder())
          .forEach(
              num -> {
                try {
                  var taskId = tasksIds.get(num);
                  service.cancelTask(taskId, user);
                } catch (RuntimeException e) {
                  logger.warn(
                      "Cannot cancel a sub task {} (task id: {}) in the flow.",
                      num,
                      tasksIds.get(num),
                      e);
                }
              });
      persistFlowState();
    }
    cmd.replyTo()
        .tell(
            StatusReply.success(
                new StreamTaskStatus(taskId, status, type, stage, percent, stagePercent)));
    return Behaviors.withTimers(
        timer -> {
          timer.startSingleTimer(timerId, new Timeout(), timeout);
          return this;
        });
  }

  private Behavior<Command> onStartFlow(Command cmd) {
    logger.info("Flow was started with flowId {}, by user {}, flow type: {}", taskId, user, type);
    if (status.equals(StreamTaskStatus.Status.INITIATED)) {
      currentTaskNumber = -1; // no tasks running
      percent = 0;
      stagePercent = 0;
      status = StreamTaskStatus.Status.IN_PROGRESS;
      if (cmd instanceof StartTask cmdStartTask) {
        descriptions = new LinkedList<>(List.of(cmdStartTask.streamTaskDescription()));
        taskWeights = List.of(1f);
        user = cmdStartTask.streamTaskDescription().getUser();
        buffer = cmdStartTask.buffer();
        parallelism = cmdStartTask.parallelism();
      } else if (cmd instanceof StartFlow cmdStartFlow) {
        descriptions = new LinkedList<>(cmdStartFlow.streamTaskDescriptions());
        onComplete = cmdStartFlow.onComplete();
        taskWeights =
            normalizeTaskWeights(
                cmdStartFlow.streamTaskDescriptions().stream()
                    .map(StreamTaskDescription::getWeight)
                    .collect(Collectors.toList()));
        user = cmdStartFlow.user();
        buffer = cmdStartFlow.buffer();
        parallelism = cmdStartFlow.parallelism();
      }
      registerForUser(user);
      startNextTask(null);
    }
    sendReplyOnStart(cmd);

    return Behaviors.withTimers(
        timer -> {
          timer.startSingleTimer(timerId, new Timeout(), timeout);
          timer.startPeriodicTimer(updateStatusTimerId, new UpdateStatus(), updatePeriod);
          return this;
        });
  }

  private Behavior<Command> onResumeFlow(ResumeFlow cmd) {
    logger.info("Flow was resumed with flowId {}, by user {}, flow type: {}", taskId, user, type);
    if (status.equals(StreamTaskStatus.Status.INITIATED)) {
      currentTaskNumber = cmd.currentTask(); // no tasks running
      percent = 0;
      stagePercent = 0;
      status = StreamTaskStatus.Status.IN_PROGRESS;
      descriptions = new LinkedList<>(cmd.streamTaskDescriptions());
      onComplete = cmd.onComplete();
      taskWeights = cmd.taskWeights();
      user = cmd.user();
      lastTaskResult = cmd.lastTaskResult();
      parallelism = cmd.parallelism();
      buffer = cmd.buffer();
      registerForUser(user);
      resumeSubTask(cmd.currentTaskId(), lastTaskResult);
    }
    sendReplyOnStart(cmd);

    return Behaviors.withTimers(
        timer -> {
          timer.startSingleTimer(timerId, new Timeout(), timeout);
          timer.startPeriodicTimer(updateStatusTimerId, new UpdateStatus(), updatePeriod);
          return this;
        });
  }

  private List<Float> normalizeTaskWeights(List<Float> taskWeights) {
    var sum = taskWeights.stream().mapToDouble(Float::doubleValue).sum();
    return taskWeights.stream().map(w -> w / (float) sum).collect(Collectors.toList());
  }

  private void sendReplyOnStart(Command cmd) {
    ActorRef<StatusReply<StreamTaskStatus>> reply = null;
    if (cmd instanceof StartTask cmdStartTask) {
      reply = cmdStartTask.replyTo();
    } else if (cmd instanceof StartFlow cmdStartFlow) {
      reply = cmdStartFlow.replyTo();
    } else if (cmd instanceof ResumeFlow cmdResumeFlow) {
      reply = cmdResumeFlow.replyTo();
    }
    Objects.requireNonNull(reply)
        .tell(
            StatusReply.success(
                new StreamTaskStatus(taskId, status, type, stage, percent, stagePercent)));
  }

  private Behavior<Command> onSubTaskComplete(SubTaskComplete cmd) {
    logger.trace("Flow task {} was completed for flow {} ", cmd.taskNumber(), taskId);
    if (!statuses.containsKey(cmd.taskNumber())) {
      try {
        statuses.put(cmd.taskNumber(), cmd.success());
        lastTaskResult = cmd.taskResult();
        closeTaskByNumber(cmd.taskNumber());
        if (!noMoreTask() && cmd.success() && status.equals(StreamTaskStatus.Status.IN_PROGRESS)) {
          startNextTask(lastTaskResult);
        } else {
          percent = 1.0f;
          stagePercent = 1.0f;
          status =
              cmd.success() && !status.equals(StreamTaskStatus.Status.COMPLETED_WITH_ERROR)
                  ? StreamTaskStatus.Status.COMPLETED
                  : StreamTaskStatus.Status.COMPLETED_WITH_ERROR;
          messages = lastTaskResult.messages();
        }
      } catch (RuntimeException e) {
        logger.error("Cannot execute next step in the flow", e);
        status = StreamTaskStatus.Status.COMPLETED_WITH_ERROR;
        percent = 1.0f;
        stagePercent = 1.0f;
      }

      executeFinalizer();
    }
    cmd.replyTo()
        .tell(
            StatusReply.success(
                new StreamTaskStatus(taskId, status, type, stage, percent, stagePercent)));
    return Behaviors.withTimers(
        timer -> {
          timer.startSingleTimer(timerId, new Timeout(), timeout);
          return this;
        });
  }

  private void executeFinalizer() {
    if (status.equals(StreamTaskStatus.Status.COMPLETED)
        || status.equals(StreamTaskStatus.Status.COMPLETED_WITH_ERROR)) {
      try {
        if (Objects.nonNull(onComplete)) {
          onComplete.accept(status.equals(StreamTaskStatus.Status.COMPLETED));
        }
      } catch (RuntimeException e) {
        logger.error("Cannot execute flow {} completion procedure ", taskId, e);
        status = StreamTaskStatus.Status.COMPLETED_WITH_ERROR;
        percent = 1.0f;
        stagePercent = 1.0f;
      }
      persistFlowState();
    }
  }

  private void closeTaskByNumber(int number) {
    var taskId = tasksIds.get(number);
    if (taskId != null) {
      service.closeTask(taskId, user);
    }
  }

  private boolean noMoreTask() {
    return currentTaskNumber >= descriptions.size() - 1;
  }

  private void startNextTask(StreamTaskResult lastTaskResult) {
    currentTaskNumber++;
    var taskDescription = descriptions.get(currentTaskNumber);
    if (Objects.nonNull(lastTaskResult) && Objects.nonNull(taskDescription.getSubscription())) {
      taskDescription.getSubscription().accept(lastTaskResult);
    }
    var newDescription = getWrappedDescription(taskDescription);
    this.stage = newDescription.getType();
    service
        .processTask(newDescription, taskId, parallelism, buffer)
        .whenComplete(
            (taskStatus, t) -> {
              if (Objects.nonNull(t)
                  || taskStatus.status().equals(StreamTaskStatus.Status.COMPLETED_WITH_ERROR)) {
                percent = 1f;
                stagePercent = 1f;
                status = StreamTaskStatus.Status.COMPLETED_WITH_ERROR;
              }
              tasksIds.put(currentTaskNumber, taskStatus.taskId());
              persistFlowState();
            });
  }

  private void resumeSubTask(String subTaskId, StreamTaskResult lastTaskResult) {
    TaskStatus subtaskStatus = persistenceService.grabSubTaskStatus(UUID.fromString(subTaskId));

    StreamTaskDescription taskDescription;
    if (Objects.isNull(subtaskStatus)) {
      taskDescription = descriptions.get(currentTaskNumber);
    } else if (!StreamTaskStatus.Status.IN_PROGRESS.equals(subtaskStatus.getStatus())) {
      if (noMoreTask()) {
        percent = 1.0f;
        stagePercent = 1.0f;
        status = subtaskStatus.getStatus();
        executeFinalizer();
        return;
      } else {
        currentTaskNumber++;
        taskDescription = descriptions.get(currentTaskNumber);
      }
    } else {
      taskDescription = persistenceService.restoreTaskFromStatus(subtaskStatus);
    }

    if (Objects.nonNull(lastTaskResult) && Objects.nonNull(taskDescription.getSubscription())) {
      taskDescription.getSubscription().accept(lastTaskResult);
    }
    var newDescription = getWrappedDescription(taskDescription);
    this.stage = newDescription.getType();
    resumeTask(newDescription, subTaskId)
        .whenComplete(
            (taskStatus, t) -> {
              if (Objects.nonNull(t)
                  || taskStatus.status().equals(StreamTaskStatus.Status.COMPLETED_WITH_ERROR)) {
                percent = 1f;
                stagePercent = 1f;
                status = StreamTaskStatus.Status.COMPLETED_WITH_ERROR;
              }
              tasksIds.put(currentTaskNumber, taskStatus.taskId());
              persistFlowState();
            });
  }

  private StreamTaskDescription getWrappedDescription(StreamTaskDescription taskDescription) {
    if (taskDescription instanceof ResumableTaskDescription resumable) {
      return new ResumableTaskDescription(
          taskDescription.getProvider(),
          taskDescription.getFunction(),
          new ResultAggregatorWrapper(taskDescription.getAggregator(), currentTaskNumber),
          resumable.getSerDe(),
          user,
          taskDescription.getType()) {
        @Override
        public DescriptionState getState() {
          return resumable.getState();
        }
      };
    }
    return new StreamTaskDescription(
        taskDescription.getProvider(),
        taskDescription.getFunction(),
        new ResultAggregatorWrapper(taskDescription.getAggregator(), currentTaskNumber),
        user,
        taskDescription.getType());
  }

  private CompletionStage<StreamTaskStatus> resumeTask(
      StreamTaskDescription task, String subTaskId) {
    return persistenceService
        .resumeTaskCommand(UUID.fromString(subTaskId))
        .thenCompose(
            actorRef ->
                persistenceService.startTaskCommand(actorRef, task, taskId, parallelism, buffer));
  }

  private void reportCompletedTask(
      boolean successful, int taskNumber, StreamTaskResult flowResult) {
    @SuppressWarnings("unused")
    CompletionStage<StreamTaskStatus> result =
        AskPattern.askWithStatus(
            getContext().getSelf(),
            replyTo -> new SubTaskComplete(replyTo, taskNumber, successful, flowResult, null),
            Duration.ofMinutes(1),
            getContext().getSystem().scheduler());
  }

  private void persistFlowState() {
    try {
      persistenceService.persistFlowState(
          onComplete,
          descriptions,
          currentTaskNumber,
          tasksIds.get(currentTaskNumber),
          taskWeights,
          lastTaskResult,
          taskId,
          status,
          type,
          user,
          parallelism,
          buffer);
    } catch (RuntimeException e) {
      logger.debug("Cannot persist a flow state: {} started by user {}", taskId, user, e);
    }
  }

  private void adjustPercent(double percent, int taskNumber) {
    getContext().getSelf().tell(new AdjustPercent(taskNumber, percent));
  }

  public record StartFlow(
      ActorRef<StatusReply<StreamTaskStatus>> replyTo,
      List<StreamTaskDescription> streamTaskDescriptions,
      Consumer<Boolean> onComplete,
      String user,
      int parallelism,
      int buffer)
      implements Command {}

  public record ResumeFlow(
      ActorRef<StatusReply<StreamTaskStatus>> replyTo,
      int currentTask,
      List<StreamTaskDescription> streamTaskDescriptions,
      List<Float> taskWeights,
      Consumer<Boolean> onComplete,
      StreamTaskResult lastTaskResult,
      String currentTaskId,
      String user,
      int parallelism,
      int buffer)
      implements Command {}

  private record SubTaskComplete(
      ActorRef<StatusReply<StreamTaskStatus>> replyTo,
      int taskNumber,
      boolean success,
      StreamTaskResult taskResult,
      Throwable error)
      implements Command {}

  private record AdjustPercent(int taskNumber, double percent) implements Command {}

  class ResultAggregatorWrapper implements ResultAggregator {

    private final ResultAggregator aggregator;
    private final int taskNumber;

    ResultAggregatorWrapper(ResultAggregator aggregator, int taskNumber) {
      this.aggregator = aggregator;
      this.taskNumber = taskNumber;
    }

    @Override
    public void consume(DataProvider.Data data) {
      aggregator.consume(data);
      adjustPercent(getPercent(), taskNumber);
    }

    @Override
    public StreamTaskResult getResult() {
      return aggregator.getResult();
    }

    @Override
    public double getPercent() {
      return aggregator.getPercent();
    }

    @Override
    public void close() {
      aggregator.close();
    }

    @Override
    public void taskCompleted(boolean successful) {
      try {
        aggregator.taskCompleted(successful);
      } catch (RuntimeException e) {
        logger.error(
            "The error has been caught for a task completion in the flow {}",
            TaskFlowActor.this.taskId,
            e);
      }
      try {
        reportCompletedTask(successful, taskNumber, getResult());
      } catch (RuntimeException e) {
        logger.error(
            "The report about the task completion was failed in the flow {}",
            TaskFlowActor.this.taskId,
            e);
      }
    }
  }
}
