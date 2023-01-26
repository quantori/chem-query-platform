package com.quantori.qdp.core.task.service;

import static com.quantori.qdp.core.source.SourceRootActor.rootActorsKey;
import static com.quantori.qdp.core.task.actor.StreamTaskActor.taskActorKey;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.javadsl.AskPattern;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import com.quantori.qdp.core.source.SourceRootActor;
import com.quantori.qdp.core.task.TaskServiceActor;
import com.quantori.qdp.core.task.actor.StreamTaskActor;
import com.quantori.qdp.core.task.actor.TaskFlowActor;
import com.quantori.qdp.core.task.model.StreamTaskDescription;
import com.quantori.qdp.core.task.model.StreamTaskDetails;
import com.quantori.qdp.core.task.model.StreamTaskProcessingException;
import com.quantori.qdp.core.task.model.StreamTaskResult;
import com.quantori.qdp.core.task.model.StreamTaskStatus;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("unused")
public class StreamTaskServiceImpl implements StreamTaskService {
    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    public static final int TIMEOUT_MILLS = 100;
    public static final int RETRY_COUNT = 600;

    private final ActorSystem<?> actorSystem;
    private final ActorRef<TaskServiceActor.Command> rootActorRef;
    private final Supplier<TaskPersistenceService> taskPersistenceServiceSupplier;

    public StreamTaskServiceImpl(ActorSystem<SourceRootActor.Command> system,
                                 ActorRef<TaskServiceActor.Command> taskActor,
                                 Supplier<TaskPersistenceService> taskPersistenceServiceSupplier) {
        this.actorSystem = system;
        this.rootActorRef = taskActor;
        this.taskPersistenceServiceSupplier = taskPersistenceServiceSupplier;
    }

    @Override
    public CompletionStage<StreamTaskStatus> getTaskStatus(String taskId, String user) {
        return getTaskActorRef(taskId).thenCompose(r -> requestStatus(r, user));
    }

    @Override
    public CompletionStage<StreamTaskResult> getTaskResult(String taskId, String user) {
        return getTaskActorRef(taskId).thenCompose(ref -> requestTaskResult(ref, user));
    }

    @Override
    public CompletionStage<StreamTaskDetails> getTaskDetails(String taskId, String user) {
        return getTaskActorRef(taskId).thenCompose(ref -> requestTaskDetails(ref, user));
    }

    @Override
    public CompletionStage<StreamTaskStatus> processTask(StreamTaskDescription task, String flowId) {
        return processTask(task, flowId, StreamTaskActor.PARALLELISM, StreamTaskActor.BUFFER_SIZE);
    }

    @Override
    public CompletionStage<StreamTaskStatus> processTask(StreamTaskDescription task, String flowId,
                                                         int parallelism, int buffer) {
      TaskPersistenceService taskPersistenceService = taskPersistenceServiceSupplier.get();

      CompletionStage<ActorRef<StreamTaskActor.Command>> stage = AskPattern.askWithStatus(
                rootActorRef,
                repl -> new TaskServiceActor.CreateTask(repl, taskPersistenceService),
                Duration.ofMinutes(1),
                actorSystem.scheduler());

        return stage.thenCompose(actorRef -> taskPersistenceService.startTaskCommand(actorRef, task, flowId,
                parallelism, buffer))
                .thenCompose(status -> {
                    if (!status.status().equals(StreamTaskStatus.Status.COMPLETED) &&
                            !status.status().equals(StreamTaskStatus.Status.COMPLETED_WITH_ERROR) &&
                            StringUtils.isNotBlank(status.taskId())) {
                        return waitAvailableActorRef(status);
                    }
                    return CompletableFuture.completedFuture(status);
                });
    }

    @Override
    public CompletionStage<StreamTaskStatus> processTaskFlowAsTask(List<StreamTaskDescription> tasks, String type,
                                                                   Consumer<Boolean> onComplete, String user) {
        return processTaskFlowAsTask(tasks, type, onComplete, user, StreamTaskActor.PARALLELISM,
                StreamTaskActor.BUFFER_SIZE);
    }

    @Override
    public CompletionStage<StreamTaskStatus> processTaskFlowAsTask(List<StreamTaskDescription> tasks, String type,
                                                                   Consumer<Boolean> onComplete, String user,
                                                                   int parallelism, int buffer) {
        CompletionStage<ActorRef<StreamTaskActor.Command>> stage = AskPattern.askWithStatus(
                rootActorRef,
                ref -> new TaskServiceActor.CreateFlow(ref, type, this, taskPersistenceServiceSupplier.get()),
                Duration.ofMinutes(1),
                actorSystem.scheduler());

        return stage.thenCompose(actorRef -> startFlowCommand(actorRef, tasks, onComplete, user, parallelism, buffer))
                .thenCompose(status -> {
                    if (!status.status().equals(StreamTaskStatus.Status.COMPLETED) &&
                            !status.status().equals(StreamTaskStatus.Status.COMPLETED_WITH_ERROR) &&
                            StringUtils.isNotBlank(status.taskId())) {
                        return waitAvailableActorRef(status);
                    }
                    return CompletableFuture.completedFuture(status);
                });
    }

    private CompletionStage<StreamTaskStatus> waitAvailableActorRef(StreamTaskStatus status) {
        int count = 0;
        boolean allFound = false;
        while (count < RETRY_COUNT && !allFound) {
            try {
                allFound = checkAllNodesReferences(status.taskId()).toCompletableFuture().get(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                logger.error("The method 'waitAvailableActorRef' was interrupted for task {}", status.taskId(), e);
                Thread.currentThread().interrupt();
                count = RETRY_COUNT;
            } catch (TimeoutException e) {
                logger.warn("Timeout error was caught in the request for a task reference {}", status.taskId());
            } catch (ExecutionException e) {
                logger.warn("Error was found in the request for a task reference {}: {}", status.taskId(), e.getMessage());
            }
            if (!allFound) {
                try {
                    Thread.sleep(TIMEOUT_MILLS);
                } catch (InterruptedException e) {
                    logger.error("The method 'waitAvailableActorRef' was interrupted in sleep for task {}", status.taskId(), e);
                    Thread.currentThread().interrupt();
                    count = RETRY_COUNT;
                }
            }
            count++;
        }
        if (allFound) {
            return CompletableFuture.completedFuture(status);
        } else {
            logger.error("The method 'waitAvailableActorRef' fails for task {} check", status.taskId());
            return CompletableFuture.completedFuture(new StreamTaskStatus(status.taskId(),
                    StreamTaskStatus.Status.COMPLETED_WITH_ERROR, 0));
        }
    }

    CompletableFuture<Boolean> checkAllNodesReferences(String taskId) {
        var serviceKey = taskActorKey(taskId);

        CompletionStage<Receptionist.Listing> cf = AskPattern.ask(
                actorSystem.receptionist(),
                ref -> Receptionist.find(rootActorsKey, ref),
                Duration.ofMinutes(1),
                actorSystem.scheduler());
        return cf.toCompletableFuture().thenCompose(listing -> {

            var results = listing.getServiceInstances(rootActorsKey).stream()
                    .map(rootRef ->
                            AskPattern.<SourceRootActor.Command, Boolean>askWithStatus(
                                    rootRef,
                                    ref -> new SourceRootActor.CheckActorReference(ref,
                                            StreamTaskActor.Command.class, taskId),
                                    Duration.ofMinutes(1),
                                    actorSystem.scheduler()).toCompletableFuture()).toList();
            return CompletableFuture.allOf(results.toArray(new CompletableFuture[0]))
                    .thenApply(v -> results.stream().allMatch(CompletableFuture::join));
        });
    }

    @Override
    public void closeTask(String taskId, String user) {
        getTaskActorRef(taskId).whenComplete((e, t) -> {
            if (Objects.isNull(t)) {
                requestClose(e, user);
            } else {
                logger.warn("Cannot close task {} , message {}", taskId, t.getMessage());
            }
        });
    }

    @Override
    public CompletionStage<StreamTaskStatus> cancelTask(String taskId, String user) {
        return getTaskActorRef(taskId)
                .thenCompose(ref -> requestCancel(ref, user));
    }

    @Override
    public CompletionStage<List<StreamTaskDetails>> getUserTaskStatus(String name) {
        var userKey = ServiceKey.create(StreamTaskActor.Command.class, Objects.requireNonNull(name));
        CompletionStage<Receptionist.Listing> cf = AskPattern.ask(
                actorSystem.receptionist(),
                ref -> Receptionist.find(userKey, ref),
                Duration.ofMinutes(1),
                actorSystem.scheduler());

        return cf.toCompletableFuture().thenCompose(listing -> {
            if (!listing.getServiceInstances(userKey).isEmpty()) {
                var results = listing.getServiceInstances(userKey)
                        .stream()
                        .map(ref -> requestTaskDetails(ref, name))
                        .map(CompletionStage::toCompletableFuture)
                        .toList();
                return CompletableFuture.allOf(results.toArray(new CompletableFuture[0]))
                        .thenApply(v -> results.stream().map(CompletableFuture::join).toList());
            }
            return CompletableFuture.completedFuture(List.of());
        });


    }

    private CompletionStage<StreamTaskStatus> startFlowCommand(ActorRef<StreamTaskActor.Command> actorRef,
                                                               List<StreamTaskDescription> streamTaskDescriptions,
                                                               Consumer<Boolean> onComplete, String user,
                                                               int parallelism, int buffer) {
        return AskPattern.askWithStatus(
                actorRef,
                replyTo -> new TaskFlowActor.StartFlow(replyTo, streamTaskDescriptions, onComplete, user,
                        parallelism, buffer),
                Duration.ofMinutes(1),
                actorSystem.scheduler());
    }

    private CompletionStage<ActorRef<StreamTaskActor.Command>> getTaskActorRef(String taskId) {
        return getTaskActorRefWithRetry(taskId, 10);
    }

    private CompletionStage<ActorRef<StreamTaskActor.Command>> getTaskActorRefWithRetry(String taskId, int retryCount) {
        ServiceKey<StreamTaskActor.Command> serviceKey = taskActorKey(taskId);

        CompletionStage<Receptionist.Listing> cf = AskPattern.ask(
                actorSystem.receptionist(),
                ref -> Receptionist.find(serviceKey, ref),
                Duration.ofMinutes(1),
                actorSystem.scheduler());

        return cf.toCompletableFuture().thenApply(listing -> {
            if (listing.getServiceInstances(serviceKey).size() != 1) {
                if (retryCount < 1) {
                    logger.error("After all retries there is no task found {}", taskId);
                    throw new StreamTaskProcessingException("Task not found: " + taskId);
                } else {
                    return findTaskRefWithTimeout(taskId, retryCount - 1);
                }
            }
            return listing.getServiceInstances(serviceKey).iterator().next();
        });
    }

    private ActorRef<StreamTaskActor.Command> findTaskRefWithTimeout(String taskId, int retryCount) {
        try {
            Thread.sleep(TIMEOUT_MILLS);
            return getTaskActorRefWithRetry(taskId, retryCount).toCompletableFuture().get(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StreamTaskProcessingException("The method findTaskRefWithTimeout was interrupted.", e);
        } catch (TimeoutException e) {
            throw new StreamTaskProcessingException("Timeout error was caught to fetch task actor reference: " + taskId);
        } catch (ExecutionException e) {
            throw new StreamTaskProcessingException("Error was caught to fetch task actor reference: " + taskId, e);
        }
    }

    private CompletionStage<StreamTaskStatus> requestStatus(ActorRef<StreamTaskActor.Command> actorRef, String user) {
        return AskPattern.askWithStatus(
                actorRef,
                ref -> new StreamTaskActor.GetStatus(ref, user),
                Duration.ofMinutes(1),
                actorSystem.scheduler());
    }

    private CompletionStage<StreamTaskResult> requestTaskResult(ActorRef<StreamTaskActor.Command> actorRef, String user) {
        return AskPattern.askWithStatus(
                actorRef,
                ref -> new StreamTaskActor.GetTaskResult(ref, user),
                Duration.ofMinutes(1),
                actorSystem.scheduler());
    }

    private CompletionStage<StreamTaskDetails> requestTaskDetails(ActorRef<StreamTaskActor.Command> actorRef, String user) {
        return AskPattern.askWithStatus(
                actorRef,
                ref -> new StreamTaskActor.GetTaskDetails(ref, user),
                Duration.ofMinutes(1),
                actorSystem.scheduler());
    }

    private void requestClose(ActorRef<StreamTaskActor.Command> actorRef, String user) {
        actorRef.tell(new StreamTaskActor.Close(user));
    }

    private CompletionStage<StreamTaskStatus> requestCancel(ActorRef<StreamTaskActor.Command> actorRef, String user) {
        return AskPattern.askWithStatus(
                actorRef,
                ref -> new StreamTaskActor.Cancel(ref, user),
                Duration.ofMinutes(1),
                actorSystem.scheduler());
    }
}
