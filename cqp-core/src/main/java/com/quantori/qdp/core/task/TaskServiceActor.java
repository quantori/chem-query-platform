package com.quantori.qdp.core.task;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.pattern.StatusReply;
import com.quantori.qdp.core.task.actor.StreamTaskActor;
import com.quantori.qdp.core.task.actor.TaskFlowActor;
import com.quantori.qdp.core.task.service.StreamTaskService;
import com.quantori.qdp.core.task.service.TaskPersistenceService;
import java.util.UUID;

public class TaskServiceActor extends AbstractBehavior<TaskServiceActor.Command> {

  private TaskServiceActor(ActorContext<Command> context) {
    super(context);
  }

  public static Behavior<Command> create() {
    return Behaviors.setup(TaskServiceActor::new);
  }

  @Override
  public Receive<Command> createReceive() {
    return newReceiveBuilder()
        .onMessage(CreateTask.class, this::onCreateTask)
        .onMessage(CreateFlow.class, this::onCreateFlow)
        .onMessage(ResumeTask.class, this::onResumeTask)
        .onMessage(ResumeFlow.class, this::onResumeFlow)
        .build();
  }

  private Behavior<Command> onCreateTask(CreateTask createTaskCmd) {
    ActorRef<StreamTaskActor.Command> taskRef =
        getContext()
            .spawn(
                StreamTaskActor.create(
                    createTaskCmd.taskPersistenceService(), createTaskCmd.replyTo),
                "task-" + UUID.randomUUID());

    getContext().getLog().debug("Created task actor taskId: {}", taskRef);
    return this;
  }

  private Behavior<Command> onResumeTask(ResumeTask resumeTaskCmd) {
    ActorRef<StreamTaskActor.Command> taskRef =
        getContext()
            .spawn(
                StreamTaskActor.create(
                    resumeTaskCmd.taskPersistenceService(),
                    resumeTaskCmd.taskId,
                    resumeTaskCmd.replyTo),
                "task-" + resumeTaskCmd.taskId);

    getContext().getLog().debug("Resume task actor: {}", taskRef);
    return this;
  }

  private Behavior<Command> onCreateFlow(CreateFlow createFlowCmd) {
    ActorRef<StreamTaskActor.Command> taskRef =
        getContext()
            .spawn(
                TaskFlowActor.create(
                    createFlowCmd.service(),
                    createFlowCmd.taskPersistenceService(),
                    createFlowCmd.type(),
                    createFlowCmd.replyTo),
                "flow-" + UUID.randomUUID());

    getContext().getLog().debug("Created flow actor: {}", taskRef);
    return this;
  }

  private Behavior<Command> onResumeFlow(ResumeFlow resumeFlowCmd) {
    ActorRef<StreamTaskActor.Command> taskRef =
        getContext()
            .spawn(
                TaskFlowActor.create(
                    resumeFlowCmd.service(),
                    resumeFlowCmd.taskPersistenceService(),
                    resumeFlowCmd.flowId(),
                    resumeFlowCmd.type(),
                    resumeFlowCmd.replyTo),
                "flow-" + resumeFlowCmd.flowId());

    getContext().getLog().debug("Resume flow actor: {}", taskRef);
    return this;
  }

  public interface Command {}

  public record CreateTask(
      ActorRef<StatusReply<ActorRef<StreamTaskActor.Command>>> replyTo,
      TaskPersistenceService taskPersistenceService)
      implements Command {}

  public record ResumeTask(
      ActorRef<StatusReply<ActorRef<StreamTaskActor.Command>>> replyTo,
      TaskPersistenceService taskPersistenceService,
      String taskId)
      implements Command {}

  public record CreateFlow(
      ActorRef<StatusReply<ActorRef<StreamTaskActor.Command>>> replyTo,
      String type,
      StreamTaskService service,
      TaskPersistenceService taskPersistenceService)
      implements Command {}

  public record ResumeFlow(
      ActorRef<StatusReply<ActorRef<StreamTaskActor.Command>>> replyTo,
      StreamTaskService service,
      TaskPersistenceService taskPersistenceService,
      String flowId,
      String type)
      implements Command {}
}
