package com.quantori.qdp.core.task.dao;

import akka.actor.typed.ActorSystem;
import akka.stream.alpakka.slick.javadsl.Slick;
import akka.stream.alpakka.slick.javadsl.SlickSession;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.quantori.qdp.core.task.model.StreamTaskDetails;
import com.quantori.qdp.core.task.model.StreamTaskStatus;
import com.quantori.qdp.core.task.model.TaskStatus;
import java.lang.invoke.MethodHandles;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskStatusDao {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  //  private final SlickSession session = SlickSession$.MODULE$.forConfig("slick-postgres");
  private final SlickSession session;
  private final ActorSystem system;

  public TaskStatusDao(SlickSession session, ActorSystem system) {
    this.session = session;
    this.system = system;
    system.classicSystem().registerOnTermination(session::close);
  }

  public List<TaskStatus> findAll() {
    try {
      return Slick.source(session,
              "SELECT id, status, task_type, restart_flag, flow_id, deserializer," +
                  //            " created_by, created_date, last_modified_date, state, parallelism, buffer FROM task_status",
                  " created_by, last_modified_date, state, parallelism, buffer FROM task_status",
              row -> TaskStatus.builder()
                  .taskId(UUID.fromString(row.nextString()))
                  .status(StreamTaskStatus.Status.valueOf(row.nextString()))
                  .type(StreamTaskDetails.TaskType.valueOf(row.nextString()))
                  .restartFlag(row.nextInt())
                  .flowId(row.nextString())
                  .deserializer(row.nextString())
                  .user(row.nextString())
                  //  .createdDate(row.nextDate())
                  .updatedDate(row.nextDate())
                  .state(row.nextString())
                  .parallelism(row.nextInt())
                  .build()
          )
          .runWith(Sink.seq(), system)
          .toCompletableFuture()
          .get();
    } catch (ExecutionException e) {
      logger.error("Could not execute findAll operation: {}", e.getMessage());

      return Collections.emptyList();
    } catch (InterruptedException e) {
      logger.error("Could not execute findAll operation: {}", e.getMessage());

      Thread.currentThread().interrupt();
      return Collections.emptyList();
    }
  }

  public void save(TaskStatus status) {
    Source.from(List.of(status))
        .runWith(Slick.sink(
            session,
            (taskStatus, connection) -> {
              PreparedStatement statement =
                  connection.prepareStatement(
                      "INSERT INTO task_statuses (id, status, task_type, restart_flag, flow_id," +
                          " deserializer, created_by, created_date, last_modified_date, state, parallelism, buffer)" +
                          " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
              statement.setString(1, taskStatus.getTaskId().toString());
              statement.setString(2, taskStatus.getStatus().toString());
              statement.setString(3, taskStatus.getType().toString());
              statement.setInt(4, taskStatus.getRestartFlag());
              statement.setString(5, taskStatus.getFlowId());
              statement.setString(6, taskStatus.getDeserializer());
              statement.setDate(7, new Date(taskStatus.getCreatedDate().getTime()));
              statement.setDate(8, new Date(taskStatus.getUpdatedDate().getTime()));
              statement.setString(9, taskStatus.getState());
              statement.setInt(10, taskStatus.getParallelism());
              statement.setInt(11, taskStatus.getBuffer());
              return statement;
            }
        ), system);
  }

  public Optional<TaskStatus> findById(UUID taskId) {
    try {
      List<TaskStatus> taskStatuses = Slick.source(session,
              "SELECT id, status, task_type, restart_flag, flow_id, deserializer," +
                  //            " created_by, created_date, last_modified_date, state, parallelism, buffer FROM task_status",
                  " created_by, last_modified_date, state, parallelism, buffer FROM task_status WHERE id = " + taskId,
              row -> TaskStatus.builder()
                  .taskId(UUID.fromString(row.nextString()))
                  .status(StreamTaskStatus.Status.valueOf(row.nextString()))
                  .type(StreamTaskDetails.TaskType.valueOf(row.nextString()))
                  .restartFlag(row.nextInt())
                  .flowId(row.nextString())
                  .deserializer(row.nextString())
                  .user(row.nextString())
                  //  .createdDate(row.nextDate())
                  .updatedDate(row.nextDate())
                  .state(row.nextString())
                  .parallelism(row.nextInt())
                  .build()
          )
          .runWith(Sink.seq(), system)
          .toCompletableFuture()
          .get();

      return taskStatuses.isEmpty()
          ? Optional.empty()
          : Optional.of(taskStatuses.get(0));
    } catch (ExecutionException e) {
      logger.error("Could not execute findById operation: {}", e.getMessage());

      return Optional.empty();
    } catch (InterruptedException e) {
      logger.error("Could not execute findById operation: {}", e.getMessage());

      Thread.currentThread().interrupt();
      return Optional.empty();
    }
  }

  public List<TaskStatus> findAllById(List<UUID> taskIds) {
    try {
      String taskIdRange = taskIds.stream()
          .map(UUID::toString)
          .collect(Collectors.joining(", ", "(", ")"));

      return Slick.source(session,
              "SELECT id, status, task_type, restart_flag, flow_id, deserializer," +
                  //            " created_by, created_date, last_modified_date, state, parallelism, buffer FROM task_status",
                  " created_by, last_modified_date, state, parallelism, buffer FROM task_status WHERE id IN " + taskIdRange,
              row -> TaskStatus.builder()
                  .taskId(UUID.fromString(row.nextString()))
                  .status(StreamTaskStatus.Status.valueOf(row.nextString()))
                  .type(StreamTaskDetails.TaskType.valueOf(row.nextString()))
                  .restartFlag(row.nextInt())
                  .flowId(row.nextString())
                  .deserializer(row.nextString())
                  .user(row.nextString())
                  //  .createdDate(row.nextDate())
                  .updatedDate(row.nextDate())
                  .state(row.nextString())
                  .parallelism(row.nextInt())
                  .build()
          )
          .runWith(Sink.seq(), system)
          .toCompletableFuture()
          .get();
    } catch (ExecutionException e) {
      logger.error("Could not execute findAllById operation: {}", e.getMessage());

      return Collections.emptyList();
    } catch (InterruptedException e) {
      logger.error("Could not execute findAllById operation: {}", e.getMessage());

      Thread.currentThread().interrupt();
      return Collections.emptyList();
    }
  }

  public void delete(TaskStatus taskStatus) {
    Slick.source(session,
            "DELETE FROM task_statuses WHERE id = " + taskStatus.getTaskId(),
            Function.identity())
        .runWith(Sink.ignore(), system);
  }

  public Optional<TaskStatus> findTaskStatusWithPessimisticLock(UUID taskId) {
    try {
      List<TaskStatus> taskStatuses = Slick.source(session,
              "SELECT id, status, task_type, restart_flag, flow_id, deserializer," +
                  //            " created_by, created_date, last_modified_date, state, parallelism, buffer FROM task_status",
                  " created_by, last_modified_date, state, parallelism, buffer FROM task_status WHERE id = " + taskId
                  + " FOR UPDATE",
              row -> TaskStatus.builder()
                  .taskId(UUID.fromString(row.nextString()))
                  .status(StreamTaskStatus.Status.valueOf(row.nextString()))
                  .type(StreamTaskDetails.TaskType.valueOf(row.nextString()))
                  .restartFlag(row.nextInt())
                  .flowId(row.nextString())
                  .deserializer(row.nextString())
                  .user(row.nextString())
                  //  .createdDate(row.nextDate())
                  .updatedDate(row.nextDate())
                  .state(row.nextString())
                  .parallelism(row.nextInt())
                  .build()
          )
          .runWith(Sink.seq(), system)
          .toCompletableFuture()
          .get();

      return taskStatuses.isEmpty()
          ? Optional.empty()
          : Optional.of(taskStatuses.get(0));
    } catch (ExecutionException e) {
      logger.error("Could not execute findById operation: {}", e.getMessage());

      return Optional.empty();
    } catch (InterruptedException e) {
      logger.error("Could not execute findById operation: {}", e.getMessage());

      Thread.currentThread().interrupt();
      return Optional.empty();
    }
  }
}
