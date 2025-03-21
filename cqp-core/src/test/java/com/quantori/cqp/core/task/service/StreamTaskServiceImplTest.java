package com.quantori.cqp.core.task.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertThrows;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.stream.alpakka.slick.javadsl.SlickSession;
import akka.stream.alpakka.slick.javadsl.SlickSession$;
import com.quantori.cqp.core.source.SourceRootActor;
import com.quantori.cqp.core.task.ContainerizedTest;
import com.quantori.cqp.core.task.TaskServiceActor;
import com.quantori.cqp.core.task.dao.TaskStatusDao;
import com.quantori.cqp.core.task.model.DataProvider;
import com.quantori.cqp.core.task.model.ResultAggregator;
import com.quantori.cqp.core.task.model.StreamTaskDescription;
import com.quantori.cqp.core.task.model.StreamTaskDetails;
import com.quantori.cqp.core.task.model.StreamTaskResult;
import com.quantori.cqp.core.task.model.StreamTaskStatus;
import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class StreamTaskServiceImplTest extends ContainerizedTest {

  private static final String TASK_TYPE = "test_task_type";
  private static final String USER = "test_user";
  private static ActorTestKit actorTestKit;
  private static StreamTaskService service;
  private static TaskPersistenceService persistenceService;

  @BeforeAll
  static void setup() {
    ActorSystem<SourceRootActor.Command> system =
        ActorSystem.create(SourceRootActor.create(100), "test-actor-system");
    actorTestKit = ActorTestKit.create(system);
    SlickSession session = SlickSession$.MODULE$.forConfig(getSlickConfig());
    system.classicSystem().registerOnTermination(session::close);
    TaskStatusDao taskStatusDao = new TaskStatusDao(session, system);

    Behavior<TaskServiceActor.Command> commandBehavior = TaskServiceActor.create();

    ActorRef<TaskServiceActor.Command> commandActorRef = actorTestKit.spawn(commandBehavior);
    service = new StreamTaskServiceImpl(system, commandActorRef, () -> persistenceService);
    persistenceService =
        new TaskPersistenceServiceImpl(
            system, commandActorRef, () -> service, taskStatusDao, new Object(), false);
  }

  @AfterEach
  void clearDb() throws IOException, InterruptedException {
    reinitTable();
  }

  @AfterAll
  static void tearDown() {
    actorTestKit.shutdownTestKit();
  }

  @Test
  void processTask() throws ExecutionException, InterruptedException {

    var expectedResult = new StreamTaskResult() {};
    var completed = new AtomicBoolean(false);
    var closed = new AtomicBoolean(false);

    var status =
        service
            .processTask(getDescription(expectedResult, completed, closed), null)
            .toCompletableFuture()
            .get();
    assertThat(status.status()).isEqualTo(StreamTaskStatus.Status.IN_PROGRESS);
    var taskId = status.taskId();

    var details = service.getTaskDetails(taskId, USER).toCompletableFuture().get();
    assertThat(details.taskId()).isEqualTo(taskId);
    assertThat(details.details()).isNotEmpty();
    assertThat(details.user()).isEqualTo(USER);

    await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> {
              var statusCheck = service.getTaskStatus(taskId, USER).toCompletableFuture().get();
              return StreamTaskStatus.Status.COMPLETED.equals(statusCheck.status());
            });

    await().atMost(Duration.ofSeconds(5)).until(completed::get);

    var result = service.getTaskResult(taskId, USER).toCompletableFuture().get();
    assertThat(result).isEqualTo(expectedResult);

    service.closeTask(taskId, USER);
    await().atMost(Duration.ofSeconds(5)).until(closed::get);
  }

  @Test
  void getAllTasksDetails() throws ExecutionException, InterruptedException {
    var expectedResult = new StreamTaskResult() {};
    var completed = new AtomicBoolean(false);
    var closed = new AtomicBoolean(false);

    var status1 =
        service
            .processTask(getDescription(expectedResult, completed, closed), null)
            .toCompletableFuture()
            .get();
    assertThat(status1.status()).isEqualTo(StreamTaskStatus.Status.IN_PROGRESS);
    var taskId1 = status1.taskId();

    var status2 =
        service
            .processTask(getDescription(expectedResult, completed, closed), null)
            .toCompletableFuture()
            .get();
    assertThat(status2.status()).isEqualTo(StreamTaskStatus.Status.IN_PROGRESS);
    var taskId2 = status1.taskId();

    var result = service.getUserTaskStatus(USER).toCompletableFuture().get();
    assertThat(result.size()).isEqualTo(2);
    var taskIds = result.stream().map(StreamTaskDetails::taskId).toList();
    assertThat(taskIds).contains(taskId1, taskId2);
  }

  @Test
  void failedFunctionTask() throws ExecutionException, InterruptedException {

    var expectedResult = new StreamTaskResult() {};
    var completed = new AtomicBoolean(false);
    var closed = new AtomicBoolean(false);

    var status =
        service
            .processTask(getDescriptionWithFailedFunction(expectedResult, completed, closed), null)
            .toCompletableFuture()
            .get();
    assertThat(status.status()).isEqualTo(StreamTaskStatus.Status.IN_PROGRESS);
    var taskId = status.taskId();

    await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> {
              var statusCheck = service.getTaskStatus(taskId, USER).toCompletableFuture().get();
              return StreamTaskStatus.Status.COMPLETED_WITH_ERROR.equals(statusCheck.status());
            });
    await().atMost(Duration.ofSeconds(5)).until(completed::get);

    assertThrows(
        ExecutionException.class,
        () -> service.getTaskResult(taskId, USER).toCompletableFuture().get());

    service.closeTask(taskId, USER);
    await().atMost(Duration.ofSeconds(5)).until(closed::get);
  }

  @Test
  void failedDataProviderTask() throws ExecutionException, InterruptedException {

    var expectedResult = new StreamTaskResult() {};
    var completed = new AtomicBoolean(false);
    var closed = new AtomicBoolean(false);

    var status =
        service
            .processTask(getDescriptionWithFailedProvider(expectedResult, completed, closed), null)
            .toCompletableFuture()
            .get();
    assertThat(status.status()).isEqualTo(StreamTaskStatus.Status.IN_PROGRESS);
    var taskId = status.taskId();

    await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> {
              var statusCheck = service.getTaskStatus(taskId, USER).toCompletableFuture().get();
              return StreamTaskStatus.Status.COMPLETED_WITH_ERROR.equals(statusCheck.status());
            });
    await().atMost(Duration.ofSeconds(5)).until(completed::get);

    assertThrows(
        ExecutionException.class,
        () -> service.getTaskResult(taskId, USER).toCompletableFuture().get());

    service.closeTask(taskId, USER);
    await().atMost(Duration.ofSeconds(5)).until(closed::get);
  }

  @Test
  void failedAggregatorTask() throws ExecutionException, InterruptedException {

    var expectedResult = new StreamTaskResult() {};
    var completed = new AtomicBoolean(false);
    var closed = new AtomicBoolean(false);

    var status =
        service
            .processTask(
                getDescriptionWithFailedAggregator(expectedResult, completed, closed), null)
            .toCompletableFuture()
            .get();
    assertThat(status.status()).isEqualTo(StreamTaskStatus.Status.IN_PROGRESS);
    var taskId = status.taskId();

    await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> {
              var statusCheck = service.getTaskStatus(taskId, USER).toCompletableFuture().get();
              return StreamTaskStatus.Status.COMPLETED_WITH_ERROR.equals(statusCheck.status());
            });
    await().atMost(Duration.ofSeconds(5)).until(completed::get);

    assertThrows(
        ExecutionException.class,
        () -> service.getTaskResult(taskId, USER).toCompletableFuture().get());

    service.closeTask(taskId, USER);
    await().atMost(Duration.ofSeconds(5)).until(closed::get);
  }

  private StreamTaskDescription getDescription(
      StreamTaskResult expectedResult, AtomicBoolean completed, AtomicBoolean closed) {

    return new StreamTaskDescription(
            () -> List.<DataProvider.Data>of(new DataProvider.Data() {}).iterator(),
            data -> new DataProvider.Data() {},
            new ResultAggregator() {
              @Override
              public void consume(DataProvider.Data data) {}

              @Override
              public StreamTaskResult getResult() {
                return expectedResult;
              }

              @Override
              public double getPercent() {
                return 0.0;
              }

              @Override
              public void close() {
                closed.set(true);
              }

              @Override
              public void taskCompleted(boolean successful) {
                completed.set(true);
              }
            },
            USER,
            TASK_TYPE)
        .setDetailsSupplier(() -> Map.of("details", "test data"));
  }

  private StreamTaskDescription getDescriptionWithFailedFunction(
      StreamTaskResult expectedResult, AtomicBoolean completed, AtomicBoolean closed) {
    return new StreamTaskDescription(
        () -> List.<DataProvider.Data>of(new DataProvider.Data() {}).iterator(),
        data -> {
          throw new RuntimeException("test exception");
        },
        new ResultAggregator() {
          @Override
          public void consume(DataProvider.Data data) {}

          @Override
          public StreamTaskResult getResult() {
            return expectedResult;
          }

          @Override
          public double getPercent() {
            return 0.0;
          }

          @Override
          public void close() {
            closed.set(true);
          }

          @Override
          public void taskCompleted(boolean successful) {
            completed.set(true);
          }
        },
        USER,
        TASK_TYPE);
  }

  private StreamTaskDescription getDescriptionWithFailedProvider(
      StreamTaskResult expectedResult, AtomicBoolean completed, AtomicBoolean closed) {
    return new StreamTaskDescription(
        () ->
            new Iterator<>() {
              @Override
              public boolean hasNext() {
                return true;
              }

              @Override
              public DataProvider.Data next() {
                throw new RuntimeException("test error");
              }
            },
        data -> data,
        new ResultAggregator() {
          @Override
          public void consume(DataProvider.Data data) {}

          @Override
          public StreamTaskResult getResult() {
            return expectedResult;
          }

          @Override
          public double getPercent() {
            return 0.0;
          }

          @Override
          public void close() {
            closed.set(true);
          }

          @Override
          public void taskCompleted(boolean successful) {
            completed.set(true);
          }
        },
        USER,
        TASK_TYPE);
  }

  private StreamTaskDescription getDescriptionWithFailedAggregator(
      StreamTaskResult expectedResult, AtomicBoolean completed, AtomicBoolean closed) {
    return new StreamTaskDescription(
        () -> List.<DataProvider.Data>of(new DataProvider.Data() {}).iterator(),
        data -> data,
        new ResultAggregator() {
          @Override
          public void consume(DataProvider.Data data) {
            throw new RuntimeException("test error");
          }

          @Override
          public StreamTaskResult getResult() {
            return expectedResult;
          }

          @Override
          public double getPercent() {
            return 0.0;
          }

          @Override
          public void close() {
            closed.set(true);
          }

          @Override
          public void taskCompleted(boolean successful) {
            completed.set(true);
          }
        },
        USER,
        TASK_TYPE);
  }
}
