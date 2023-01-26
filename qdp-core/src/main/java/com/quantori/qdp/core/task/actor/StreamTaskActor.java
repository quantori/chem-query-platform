package com.quantori.qdp.core.task.actor;

import akka.NotUsed;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.javadsl.TimerScheduler;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import akka.pattern.StatusReply;
import akka.stream.ActorAttributes;
import akka.stream.Attributes;
import akka.stream.OverflowStrategy;
import akka.stream.Supervision;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.typed.javadsl.ActorSink;
import com.quantori.qdp.core.task.model.DataProvider;
import com.quantori.qdp.core.task.model.ResumableTaskDescription;
import com.quantori.qdp.core.task.model.StreamTaskDescription;
import com.quantori.qdp.core.task.model.StreamTaskDetails;
import com.quantori.qdp.core.task.model.StreamTaskProcessingException;
import com.quantori.qdp.core.task.model.StreamTaskResult;
import com.quantori.qdp.core.task.model.StreamTaskStatus;
import com.quantori.qdp.core.task.service.TaskPersistenceService;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class StreamTaskActor extends AbstractBehavior<StreamTaskActor.Command> {
    public static final int PARALLELISM = 1;
    public static final int BUFFER_SIZE = 1;
    private static final Logger logger = LoggerFactory.getLogger(StreamTaskActor.class);

    public enum Ack {
        INSTANCE
    }

    protected final Duration timeout = Duration.ofMinutes(10);
    protected final Duration updatePeriod = Duration.ofSeconds(30);
    protected final String timerId;
    protected final String updateStatusTimerId;
    protected final String taskId;
    protected final TaskPersistenceService persistenceService;
    protected String type;
    protected double percent = 0.0;
    protected double stagePercent = 0.0;
    protected int parallelism = PARALLELISM;
    protected int buffer = BUFFER_SIZE;
    protected String flowId;
    protected StreamTaskStatus.Status status = StreamTaskStatus.Status.INITIATED;
    protected List<String> messages;
    protected LocalDateTime created = LocalDateTime.now();
    private StreamTaskDescription streamTaskDescription;

    protected StreamTaskActor(ActorContext<Command> context, TimerScheduler<Command> timerScheduler,
                              TaskPersistenceService persistenceService,
                              ActorRef<StatusReply<ActorRef<Command>>> replyTo) {
        this(context, timerScheduler, persistenceService, UUID.randomUUID().toString(), replyTo);
    }

    protected StreamTaskActor(ActorContext<Command> context, TimerScheduler<Command> timerScheduler,
                              TaskPersistenceService persistenceService, String taskId,
                              ActorRef<StatusReply<ActorRef<Command>>> replyTo) {
        super(context);
        this.taskId = taskId;
        this.timerId = UUID.randomUUID().toString();
        this.updateStatusTimerId = UUID.randomUUID().toString();
        this.persistenceService = persistenceService;

        timerScheduler.startSingleTimer(timerId, new Timeout(), timeout);

        registerSearchActor(context, replyTo, taskId);
    }

    public static Behavior<Command> create(TaskPersistenceService persistenceService,
                                           ActorRef<StatusReply<ActorRef<Command>>> replyTo) {
        return Behaviors.setup(ctx ->
                Behaviors.withTimers(timer -> new StreamTaskActor(ctx, timer, persistenceService, replyTo)));
    }

    public static Behavior<Command> create(TaskPersistenceService persistenceService, String taskId,
                                           ActorRef<StatusReply<ActorRef<Command>>> replyTo) {
        return Behaviors.setup(ctx ->
                Behaviors.withTimers(timer -> new StreamTaskActor(ctx, timer, persistenceService, taskId, replyTo)));
    }

    private void registerSearchActor(ActorContext<Command> context,
                                     ActorRef<StatusReply<ActorRef<Command>>> replyTo,
                                     String taskId) {
        var serviceKey = taskActorKey(taskId);
        var listener = Behaviors.receive(Receptionist.Registered.class)
                .onMessage(Receptionist.Registered.class, msg -> {
                    if (msg.getKey().id().equals(taskId)) {
                        replyTo.tell(StatusReply.success(context.getSelf()));
                        return Behaviors.stopped();
                    }
                    return Behaviors.same();
                })
                .build();
        var refListener = getContext().spawn(listener,
                "task-registerer-" + UUID.randomUUID().toString());

        // Register this actor in Receptionist to make it globally discoverable.
        context.getSystem().receptionist().tell(Receptionist.register(serviceKey, context.getSelf(), refListener));
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(StartTask.class, this::onStart)
                .onMessage(GetStatus.class, this::onStatus)
                .onMessage(GetTaskResult.class, this::onTaskResult)
                .onMessage(GetTaskDetails.class, this::onTaskDetails)
                .onMessage(Timeout.class, this::onTimeout)
                .onMessage(Close.class, this::onClose)
                .onMessage(Cancel.class, this::onCancel)
                .onMessage(StreamCompleted.class, this::onComplete)
                .onMessage(StreamInitialized.class, this::onInitFlow)
                .onMessage(Item.class, this::onItem)
                .onMessage(StreamFailure.class, this::onFailure)
                .onMessage(UpdateStatus.class, this::onUpdateStatus)
                .build();
    }

    private Behavior<Command> onStart(StartTask startTask) {
        var taskDescription = startTask.streamTaskDescription();
        logger.info("Start task initial request with taskId: {}, by user {}, in flow: {}, task type: {}",
                taskId, taskDescription.getUser(), flowId, taskDescription.getType());
        if (this.status.equals(StreamTaskStatus.Status.INITIATED)) {
            this.status = StreamTaskStatus.Status.IN_PROGRESS;
            this.streamTaskDescription = taskDescription;
            this.parallelism = startTask.parallelism();
            this.buffer = startTask.buffer();
            this.flowId = startTask.flowId();
            this.type = streamTaskDescription.getType();
            if (Objects.isNull(flowId)) {
                registerForUser(streamTaskDescription.getUser());
            }
            try {
                var iterator = streamTaskDescription.getProvider().dataIterator();
                runFlow(iterator);
            } catch (RuntimeException e) {
                logger.error("Error to start task with Id: {}, cmd : {} (user: {})", taskId, startTask,
                        streamTaskDescription.getUser(), e);
                status = StreamTaskStatus.Status.COMPLETED_WITH_ERROR;
                completed(false);
            }
            persistTaskState();
            startTask.replyTo.tell(StatusReply.success(new StreamTaskStatus(taskId,
                    status,
                    0)));
        }
        return Behaviors.withTimers(timer -> {
            timer.startSingleTimer(timerId, new Timeout(), timeout);
            timer.startPeriodicTimer(updateStatusTimerId, new UpdateStatus(), updatePeriod);
            return this;
        });
    }

    protected void registerForUser(String user) {
        var userKey = ServiceKey.create(Command.class,
                Objects.requireNonNull(user));

        getContext().getSystem().receptionist().tell(Receptionist.register(userKey, getContext().getSelf()));
    }

    private void persistTaskState() {
        try {
            if (streamTaskDescription instanceof ResumableTaskDescription resumableTaskDescription) {
                persistenceService.persistTask(UUID.fromString(taskId), status, resumableTaskDescription,
                        flowId, parallelism, buffer);
            }
        } catch (RuntimeException e) {
            logger.debug("Cannot persist a task state: {} started by user {}", taskId,
                    streamTaskDescription.getUser(), e);
        }
    }

    Behavior<Command> onStatus(GetStatus getStatus) {
        logger.trace("Status request: {}", getStatus);
        if (Objects.nonNull(streamTaskDescription) && !getStatus.user.equals(streamTaskDescription.getUser())) {
            logger.warn("Task details access violation for user: {}, taskId {}", getStatus.user(), taskId);
            getStatus.replyTo.tell(StatusReply.error(new StreamTaskProcessingException(
                    "Task details access violation for user: " + getStatus.user())));
            return this;
        }
        getStatus.replyTo.tell(StatusReply.success(new StreamTaskStatus(taskId,
                status,
                type,
                percent,
                stagePercent,
                messages)));
        return Behaviors.withTimers(timer -> {
            timer.startSingleTimer(timerId, new Timeout(), timeout);
            return this;
        });
    }

    private Behavior<Command> onTaskResult(GetTaskResult getTaskResult) {
        logger.trace("Result request: {}", getTaskResult);
        if (!getTaskResult.user.equals(streamTaskDescription.getUser())) {
            logger.warn("Task result access violation for user: {}, taskId {}", getTaskResult.user(), taskId);
            getTaskResult.replyTo.tell(StatusReply.error(new StreamTaskProcessingException(
                    "Task result access violation for user: " + getTaskResult.user())));
            return this;
        }
        switch (status) {
            case COMPLETED -> getTaskResult.replyTo
                    .tell(StatusReply.success(streamTaskDescription.getAggregator().getResult()));
            case COMPLETED_WITH_ERROR -> getTaskResult.replyTo.tell(StatusReply.error(
                    new StreamTaskProcessingException("Task execution was failed")));
            default -> getTaskResult.replyTo
                    .tell(StatusReply.error(new StreamTaskProcessingException("Task is not ready")));
        }
        return Behaviors.withTimers(timer -> {
            timer.startSingleTimer(timerId, new Timeout(), timeout);
            return this;
        });
    }

    private Behavior<Command> onTaskDetails(GetTaskDetails getTaskDetails) {
        logger.trace("Result request: {}", getTaskDetails);
        if (!getTaskDetails.user.equals(streamTaskDescription.getUser())) {
            logger.warn("Task details access violation for user: {}, taskId {}", getTaskDetails.user(), taskId);
            getTaskDetails.replyTo.tell(StatusReply.error(new StreamTaskProcessingException(
                    "Task details access violation for user: " + getTaskDetails.user())));
            return this;
        }
        var details = streamTaskDescription.getDetailsMapProvider().get();
        getTaskDetails.replyTo.tell(StatusReply.success(
                new StreamTaskDetails(taskId, type, streamTaskDescription.getUser(), created, details, status)
        ));

        return Behaviors.withTimers(timer -> {
            timer.startSingleTimer(timerId, new Timeout(), timeout);
            return this;
        });
    }

    private Behavior<Command> onTimeout(Timeout cmd) {
        logger.debug("Reached task execution timeout (will stop actor): {}", getContext().getSelf());
        close();
        return Behaviors.stopped();
    }

    private Behavior<Command> onClose(Close cmd) {
        if (streamTaskDescription.getUser().equals(cmd.user())) {
            logger.debug("Close command was received for the task: {}", getContext().getSelf());
            close();
            return Behaviors.stopped();
        } else {
            logger.warn("No access for user {} into the task {}", cmd.user, taskId);
            return this;
        }
    }

    private Behavior<Command> onCancel(Cancel cmd) {
        logger.info("Stream task was canceled with taskId {}, user: {}", taskId, streamTaskDescription.getUser());
        if (streamTaskDescription.getUser().equals(cmd.user())) {
            if (!status.equals(StreamTaskStatus.Status.COMPLETED_WITH_ERROR)
                    && !status.equals(StreamTaskStatus.Status.COMPLETED)) {
                logger.debug("Cancel command was received for the task: {}", getContext().getSelf());
                this.status = StreamTaskStatus.Status.COMPLETED_WITH_ERROR;
                persistTaskState();
            }
            cmd.replyTo.tell(StatusReply.success(new StreamTaskStatus(taskId,
                    status,
                    type,
                    percent,
                    stagePercent)));
            return Behaviors.withTimers(timer -> {
                timer.startSingleTimer(timerId, new Timeout(), timeout);
                return this;
            });
        } else {
            logger.warn("No access for user {} into the task {}", cmd.user, taskId);
            cmd.replyTo.tell(StatusReply.error(new StreamTaskProcessingException(
                    "Task cancellation operation access violation for user: " + cmd.user())));
            return this;
        }
    }

    private Behavior<Command> onComplete(StreamCompleted cmd) {
        logger.info("Stream task completed with taskId {}, user: {}, status: {}", taskId,
                streamTaskDescription.getUser(), status);
        if (status.equals(StreamTaskStatus.Status.COMPLETED_WITH_ERROR)) {
            completed(false);
        } else {
            completed(true);
            this.status = StreamTaskStatus.Status.COMPLETED;
        }
        percent = 1.0f;
        stagePercent = 1.0f;
        persistTaskState();
        // Register timer to terminate this actor in case of inactivity longer than timeout.
        return Behaviors.withTimers(timer -> {
            timer.startSingleTimer(timerId, new Timeout(), timeout);
            return this;
        });
    }

    private Behavior<Command> onInitFlow(StreamInitialized cmd) {
        logger.debug("Task stream initialized");
        cmd.replyTo.tell(Ack.INSTANCE);
        return Behaviors.withTimers(timer -> {
            timer.startSingleTimer(timerId, new Timeout(), timeout);
            return this;
        });
    }

    private Behavior<Command> onItem(Item element) {
        logger.trace("Task flow element: {}", element);
        if (status.equals(StreamTaskStatus.Status.IN_PROGRESS)) {
            try {
                streamTaskDescription.getAggregator().consume(element.data);
                percent = streamTaskDescription.getAggregator().getPercent();
                stagePercent = percent;
            } catch (RuntimeException e) {
                logger.error("Error in task {} started by user {}", taskId, streamTaskDescription.getUser(), e);
                status = StreamTaskStatus.Status.COMPLETED_WITH_ERROR;
                messages = List.of(e.getMessage());
            }
            element.replyTo.tell(Ack.INSTANCE);
        }
        return Behaviors.withTimers(timer -> {
            timer.startSingleTimer(timerId, new Timeout(), timeout);
            return this;
        });
    }

    protected Behavior<Command> onUpdateStatus(UpdateStatus cmd) {
        if (!status.equals(StreamTaskStatus.Status.COMPLETED)
                && !status.equals(StreamTaskStatus.Status.COMPLETED_WITH_ERROR)) {
            persistenceService.updateStatus(taskId);
        }
        return this;
    }

    private Behavior<Command> onFailure(StreamFailure failed) {
        logger.error("Task stream failed! Task id: {}, User: {}", taskId,
                streamTaskDescription.getUser(), failed.cause);
        completed(false);
        this.status = StreamTaskStatus.Status.COMPLETED_WITH_ERROR;
        persistTaskState();
        // Register timer to terminate this actor in case of inactivity longer than timeout.
        return Behaviors.withTimers(timer -> {
            timer.startSingleTimer(timerId, new Timeout(), timeout);
            return this;
        });
    }

    public static ServiceKey<Command> taskActorKey(String taskId) {
        return ServiceKey.create(Command.class, Objects.requireNonNull(taskId));
    }

    private void close() {
        try {
            streamTaskDescription.getProvider().close();
        } catch (RuntimeException e) {
            logger.error("Cannot close data provider resources", e);
        }
        try {
            streamTaskDescription.getAggregator().close();
        } catch (RuntimeException e) {
            logger.error("Cannot close aggregated resources", e);
        }
    }

    private void completed(boolean successful) {
        try {
            streamTaskDescription.getProvider().taskCompleted(successful);
        } catch (RuntimeException e) {
            logger.error("Cannot close data provider resources", e);
        }
        try {
            streamTaskDescription.getAggregator().taskCompleted(successful);
        } catch (RuntimeException e) {
            logger.error("Cannot close aggregated resources", e);
        }
    }

    public interface Command {
    }

    public static record StartTask(
            ActorRef<StatusReply<StreamTaskStatus>> replyTo,
            StreamTaskDescription streamTaskDescription, String flowId,
            int parallelism, int buffer) implements Command {
    }

    public static class Timeout implements Command {
    }

    public static record GetStatus(
            ActorRef<StatusReply<StreamTaskStatus>> replyTo, String user) implements Command {
    }

    private static record StreamInitialized(
            ActorRef<Ack> replyTo) implements Command {
    }

    public static record GetTaskResult(
            ActorRef<StatusReply<StreamTaskResult>> replyTo, String user) implements Command {
    }

    public static record GetTaskDetails(
            ActorRef<StatusReply<StreamTaskDetails>> replyTo, String user) implements Command {
    }

    private static class StreamCompleted implements Command {
    }

    private static record Item(ActorRef<Ack> replyTo,
                               DataProvider.Data data) implements Command {
    }

    private static record StreamFailure(Throwable cause) implements Command {
    }

    protected static record UpdateStatus() implements Command {
    }

    public static record Close(String user) implements Command {
    }

    public static record Cancel(ActorRef<StatusReply<StreamTaskStatus>> replyTo, String user) implements Command {
    }

    private void runFlow(Iterator<? extends DataProvider.Data> iterator) {
        logger.debug("The task stream was asked to run {} by user {}", taskId, streamTaskDescription.getUser());

        var source = getSource(iterator);
        var transStep = addFlowStep(source);

        transStep
                .toMat(getSink(), Keep.right())
                .withAttributes(ActorAttributes.withSupervisionStrategy(Supervision.getStoppingDecider()))
                .addAttributes(Attributes.inputBuffer(1, 1))
                .run(getContext().getSystem());
        logger.debug("The task stream was initiated {}", taskId);
    }

    private Source<? extends DataProvider.Data, NotUsed> getSource(Iterator<? extends DataProvider.Data> iterator) {
        return Source.fromIterator(() -> iterator
        ).withAttributes(ActorAttributes.withSupervisionStrategy(Supervision.getStoppingDecider()));
    }

    private Sink<DataProvider.Data, NotUsed> getSink() {
        final StreamCompleted completeMessage = new StreamCompleted();
        return ActorSink.actorRefWithBackpressure(
                getContext().getSelf(),
                Item::new,
                StreamInitialized::new,
                Ack.INSTANCE,
                completeMessage,
                StreamFailure::new
        ).withAttributes(ActorAttributes.withSupervisionStrategy(Supervision.getStoppingDecider()));
    }

    private Source<DataProvider.Data, NotUsed> addFlowStep(Source<? extends DataProvider.Data, NotUsed> source) {
        return source.takeWhile(e -> status.equals(StreamTaskStatus.Status.IN_PROGRESS))
                .mapAsync(parallelism, data -> CompletableFuture.supplyAsync(() -> {
                    logger.trace("Data was accepted : {}", data);
                    return streamTaskDescription.getFunction().apply(data);
                }))
                .buffer(buffer, OverflowStrategy.backpressure())
                .withAttributes(ActorAttributes.withSupervisionStrategy(Supervision.getStoppingDecider()));
    }
}