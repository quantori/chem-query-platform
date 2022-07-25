package com.quantori.qdp.core.task.dao;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.stream.alpakka.slick.javadsl.Slick;
import akka.stream.alpakka.slick.javadsl.SlickRow;
import akka.stream.alpakka.slick.javadsl.SlickSession;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.quantori.qdp.core.task.model.StreamTaskDetails;
import com.quantori.qdp.core.task.model.StreamTaskStatus;
import com.quantori.qdp.core.task.model.TaskStatus;
import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TaskStatusDao {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private final SlickSession session;
  private final ActorSystem<?> system;

  public TaskStatusDao(SlickSession session, ActorSystem<?> system) {
    this.session = session;
    this.system = system;
    system.classicSystem().registerOnTermination(session::close);
  }

  public List<TaskStatus> findAll() {
    try {
      return Slick.source(session,
              "SELECT id, status, type, restart_flag, flow_id, deserializer," +
                  " created_by, created_date, updated_date, state, parallelism, buffer FROM task_statuses",
              this::buildTaskStatus
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
    try {
      Sink<TaskStatus, CompletionStage<Done>> sink = Slick.sink(
          session,
          (taskStatus, connection) -> {
            PreparedStatement statement =
                connection.prepareStatement(
                    "INSERT INTO task_statuses (id, status, type, restart_flag, flow_id," +
                        " deserializer, created_by, created_date, updated_date, state, parallelism, buffer)" +
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" +
                        " ON CONFLICT (id) " +
                        " DO UPDATE SET" +
                        " status=?, type=?, restart_flag=?, flow_id=?, deserializer=?, created_by=?," +
                        " updated_date=?, state=?, parallelism=?, buffer=?");
            statement.setObject(1, taskStatus.getTaskId());
            statement.setString(2, taskStatus.getStatus().toString());
            statement.setString(13, taskStatus.getStatus().toString());
            statement.setString(3, taskStatus.getType().toString());
            statement.setString(14, taskStatus.getType().toString());
            statement.setInt(4, taskStatus.getRestartFlag());
            statement.setInt(15, taskStatus.getRestartFlag());
            statement.setString(5, taskStatus.getFlowId());
            statement.setString(16, taskStatus.getFlowId());
            statement.setString(6, taskStatus.getDeserializer());
            statement.setString(17, taskStatus.getDeserializer());
            statement.setString(7, taskStatus.getUser());
            statement.setString(18, taskStatus.getUser());
            if (taskStatus.getCreatedDate() == null) {
              statement.setTimestamp(8, Timestamp.from(Instant.now()));
            } else {
              statement.setTimestamp(8, new Timestamp(taskStatus.getCreatedDate().getTime()));
            }
            if (taskStatus.getUpdatedDate() == null) {
              Timestamp timestamp = Timestamp.from(Instant.now());
              statement.setTimestamp(9, timestamp);
              statement.setTimestamp(19, timestamp);
            } else {
              Timestamp timestamp = new Timestamp(taskStatus.getUpdatedDate().getTime());
              statement.setTimestamp(9, timestamp);
              statement.setTimestamp(19, timestamp);
            }
            statement.setString(10, taskStatus.getState());
            statement.setString(20, taskStatus.getState());
            statement.setInt(11, taskStatus.getParallelism());
            statement.setInt(21, taskStatus.getParallelism());
            statement.setInt(12, taskStatus.getBuffer());
            statement.setInt(22, taskStatus.getBuffer());

            return statement;
          }
      );

      Source.from(List.of(status))
          .runWith(sink, system)
          .toCompletableFuture().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error(e.getMessage());
    } catch (ExecutionException e) {
      logger.error(e.getMessage());
    }
  }

  public Optional<TaskStatus> findById(UUID taskId) {
    try {
      List<TaskStatus> taskStatuses = Slick.source(session,
              "SELECT id, status, type, restart_flag, flow_id, deserializer," +
                  " created_by, created_date, updated_date, state, parallelism, buffer " +
                  "FROM task_statuses WHERE id = '" + taskId + "'",
              this::buildTaskStatus
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
          .collect(Collectors.joining("', '", "('", "')"));

      return Slick.source(session,
              "SELECT id, status, type, restart_flag, flow_id, deserializer," +
                  " created_by, created_date, updated_date, state, parallelism, buffer " +
                  "FROM task_statuses WHERE id IN " + taskIdRange,
              this::buildTaskStatus
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
            "DELETE FROM task_statuses WHERE id = '" + taskStatus.getTaskId() + "'",
            Function.identity())
        .runWith(Sink.ignore(), system);
  }

  public Optional<TaskStatus> findTaskStatusWithPessimisticLock(UUID taskId) {
    try {
      List<TaskStatus> taskStatuses = Slick.source(session,
              "SELECT id, status, type, restart_flag, flow_id, deserializer," +
                  " created_by, created_date, updated_date, state, parallelism, buffer FROM task_statuses WHERE id = '" +
                  taskId + "' FOR UPDATE",
              this::buildTaskStatus
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

  private TaskStatus buildTaskStatus(SlickRow row) {
    return TaskStatus.builder()
        .taskId(UUID.fromString(row.nextString()))
        .status(StreamTaskStatus.Status.valueOf(row.nextString()))
        .type(StreamTaskDetails.TaskType.valueOf(row.nextString()))
        .restartFlag(row.nextInt())
        .flowId(row.nextString())
        .deserializer(row.nextString())
        .user(row.nextString())
        .createdDate(new Date(row.nextTimestamp().getTime()))
        .updatedDate(new Date(row.nextTimestamp().getTime()))
        .state(row.nextString())
        .parallelism(row.nextInt())
        .buffer(row.nextInt())
        .build();
  }
}
