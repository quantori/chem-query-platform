package com.quantori.qdp.core.task.dao;

import akka.Done;
import akka.actor.typed.ActorSystem;
import akka.stream.alpakka.slick.javadsl.Slick;
import akka.stream.alpakka.slick.javadsl.SlickRow;
import akka.stream.alpakka.slick.javadsl.SlickSession;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import com.quantori.qdp.core.task.model.StreamTaskDetails;
import com.quantori.qdp.core.task.model.StreamTaskProcessingException;
import com.quantori.qdp.core.task.model.StreamTaskStatus;
import com.quantori.qdp.core.task.model.TaskStatus;
import java.lang.invoke.MethodHandles;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
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
              "SELECT id, status, task_type, restart_flag, flow_id, deserializer," +
                  " created_by, created_date, updated_date, state, parallelism, buffer FROM task_status",
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
          this::getSavePreparedStatement
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
              "SELECT id, status, task_type, restart_flag, flow_id, deserializer," +
                  " created_by, created_date, updated_date, state, parallelism, buffer " +
                  "FROM task_status WHERE id = '" + taskId + "'",
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
              "SELECT id, status, task_type, restart_flag, flow_id, deserializer," +
                  " created_by, created_date, updated_date, state, parallelism, buffer " +
                  "FROM task_status WHERE id IN " + taskIdRange,
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
            "DELETE FROM task_status WHERE id = '" + taskStatus.getTaskId() + "'",
            Function.identity())
        .runWith(Sink.ignore(), system);
  }

  public void deleteAll() {
    Slick.source(session,
        "DELETE FROM task_status", Function.identity())
        .runWith(Sink.ignore(), system);
  }

  public Optional<TaskStatus> findTaskStatusWithPessimisticLock(UUID taskId) {
    try {
      List<TaskStatus> taskStatuses = Slick.source(session,
              "SELECT id, status, task_type, restart_flag, flow_id, deserializer," +
                  " created_by, created_date, updated_date, state, parallelism, buffer FROM task_status WHERE id = '" +
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

  public TaskStatus grabSubTaskStatus(UUID taskId) {
    try (Connection connection = session.db().source().createConnection()) {
      connection.setAutoCommit(false);

      TaskStatus result = findTaskStatusWithPessimisticLock(taskId, connection).orElse(null);

      if (Objects.nonNull(result)) {
        if (result.getRestartFlag() > 0) {
          throw new StreamTaskProcessingException("The task was already restarted: " + taskId);
        }
        if (StreamTaskStatus.Status.IN_PROGRESS.equals(result.getStatus())) {
          result.setRestartFlag(result.getRestartFlag() + 1);
          save(result, connection);
        }
      }

      connection.commit();

      return result;
    } catch (SQLException e) {
      throw new StreamTaskProcessingException("Could not execute transaction grabSubTaskStatus for task: " + taskId);
    }
  }

  public TaskStatus markForTaskExecution(UUID taskId) {
    try (Connection connection = session.db().source().createConnection()) {
      connection.setAutoCommit(false);

      TaskStatus result = findTaskStatusWithPessimisticLock(taskId, connection).orElseThrow(
          () -> new StreamTaskProcessingException("No task to resume " + taskId));

      if (result.getRestartFlag() > 0 || !StreamTaskStatus.Status.IN_PROGRESS.equals(result.getStatus())) {
        throw new StreamTaskProcessingException("The task was already restarted or was completed: " + taskId);
      }

      result.setRestartFlag(result.getRestartFlag() + 1);
      save(result, connection);

      connection.commit();

      return result;
    } catch (SQLException e) {
      throw new StreamTaskProcessingException("Could not execute transaction markForTaskExecution for task: " + taskId);
    }
  }

  private Optional<TaskStatus> findTaskStatusWithPessimisticLock(UUID taskId, Connection connection)
      throws SQLException {
    try (PreparedStatement preparedStatement =
             connection.prepareStatement("SELECT id, status, task_type, restart_flag, flow_id, deserializer," +
                 " created_by, created_date, updated_date, state, parallelism, buffer FROM task_status WHERE id = ? FOR UPDATE")) {
      preparedStatement.setObject(1, taskId);

      try {
        return Optional.ofNullable(buildTaskStatus(preparedStatement.executeQuery()));
      } catch (SQLException e) {
        logger.error("Could not execute findTaskStatusWithPessimisticLock operation for task: " + taskId);

        return Optional.empty();
      }
    }
  }

  private void save(TaskStatus taskStatus, Connection connection) throws SQLException {
    try (PreparedStatement preparedStatement = getSavePreparedStatement(taskStatus, connection)) {
      preparedStatement.execute();
    }
  }

  private PreparedStatement getSavePreparedStatement(TaskStatus taskStatus, Connection connection) throws SQLException {
    PreparedStatement statement =
        connection.prepareStatement(
            "INSERT INTO task_status (id, status, task_type, restart_flag, flow_id," +
                " deserializer, created_by, created_date, updated_date, state, parallelism, buffer)" +
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" +
                " ON CONFLICT (id) " +
                " DO UPDATE SET" +
                " status=?, task_type=?, restart_flag=?, flow_id=?, deserializer=?, created_by=?," +
                " updated_date=?, state=?, parallelism=?, buffer=?");

      statement.setObject(1, taskStatus.getTaskId());
      statement.setInt(2, taskStatus.getStatus().ordinal());
      statement.setInt(13, taskStatus.getStatus().ordinal());
      statement.setInt(3, taskStatus.getType().ordinal());
      statement.setInt(14, taskStatus.getType().ordinal());
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
      Timestamp timestamp;
      if (taskStatus.getUpdatedDate() == null) {
        timestamp = Timestamp.from(Instant.now());
      } else {
        timestamp = new Timestamp(taskStatus.getUpdatedDate().getTime());
      }
      statement.setTimestamp(9, timestamp);
      statement.setTimestamp(19, timestamp);
      statement.setString(10, taskStatus.getState());
      statement.setString(20, taskStatus.getState());
      statement.setInt(11, taskStatus.getParallelism());
      statement.setInt(21, taskStatus.getParallelism());
      statement.setInt(12, taskStatus.getBuffer());
      statement.setInt(22, taskStatus.getBuffer());

      return statement;
  }

  private TaskStatus buildTaskStatus(SlickRow row) {
    return TaskStatus.builder()
        .taskId(UUID.fromString(row.nextString()))
        .status(StreamTaskStatus.Status.values()[row.nextInt()])
        .type(StreamTaskDetails.TaskType.values()[row.nextInt()])
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

  private TaskStatus buildTaskStatus(ResultSet resultSet) throws SQLException {
    resultSet.next();

    return TaskStatus.builder()
        .taskId(UUID.fromString(resultSet.getString("id")))
        .status(StreamTaskStatus.Status.values()[resultSet.getInt("status")])
        .type(StreamTaskDetails.TaskType.values()[resultSet.getInt("task_type")])
        .restartFlag(resultSet.getInt("restart_flag"))
        .flowId(resultSet.getString("flow_id"))
        .deserializer(resultSet.getString("deserializer"))
        .user(resultSet.getString("created_by"))
        .createdDate(new Date(resultSet.getTimestamp("created_date").getTime()))
        .updatedDate(new Date(resultSet.getTimestamp("updated_date").getTime()))
        .state(resultSet.getString("state"))
        .parallelism(resultSet.getInt("parallelism"))
        .buffer(resultSet.getInt("buffer"))
        .build();
  }
}
