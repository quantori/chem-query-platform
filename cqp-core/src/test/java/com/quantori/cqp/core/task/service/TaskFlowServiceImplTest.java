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
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TaskFlowServiceImplTest extends ContainerizedTest {

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
  void processFlow() throws ExecutionException, InterruptedException {

    var expectedResult = new StreamTaskResult() {};
    var completed = new AtomicInteger(0);
    var closed = new AtomicInteger(0);
    var onComplete = new AtomicInteger(0);

    var descriptions =
        List.of(
            getDescription(expectedResult, completed, closed),
            getDescription(null, completed, closed),
            getDescription(null, completed, closed));

    var status =
        service
            .processTaskFlowAsTask(descriptions, TASK_TYPE, e -> onComplete.incrementAndGet(), USER)
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

    await().atMost(Duration.ofSeconds(50)).until(() -> completed.get() == 3);

    var result = service.getTaskResult(taskId, USER).toCompletableFuture().get();
    assertThat(result).isEqualTo(expectedResult);

    service.closeTask(taskId, USER);
    await().atMost(Duration.ofSeconds(5)).until(() -> closed.get() == 3);
    assertThat(onComplete.get()).isEqualTo(1);
  }

  @Test
  void getAllFlowsDetails() throws ExecutionException, InterruptedException {
    var completed = new AtomicInteger();
    var closed = new AtomicInteger();
    var onComplete = new AtomicInteger(0);

    var descriptions =
        List.of(
            getDescription(null, completed, closed),
            getDescription(null, completed, closed),
            getDescription(null, completed, closed));

    var status1 =
        service
            .processTaskFlowAsTask(descriptions, TASK_TYPE, e -> onComplete.incrementAndGet(), USER)
            .toCompletableFuture()
            .get();
    assertThat(status1.status()).isEqualTo(StreamTaskStatus.Status.IN_PROGRESS);
    var taskId1 = status1.taskId();

    var status2 =
        service
            .processTaskFlowAsTask(descriptions, TASK_TYPE, e -> onComplete.incrementAndGet(), USER)
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
    var completed = new AtomicInteger(0);
    var closed = new AtomicInteger(0);

    var descriptions =
        List.of(
            getDescription(expectedResult, completed, closed),
            getDescriptionWithFailedFunction(expectedResult, completed, closed),
            getDescription(null, completed, closed));

    var status =
        service
            .processTaskFlowAsTask(descriptions, TASK_TYPE, e -> {}, USER)
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
    await().atMost(Duration.ofSeconds(5)).until(() -> completed.get() == 2);

    assertThrows(
        ExecutionException.class,
        () -> service.getTaskResult(taskId, USER).toCompletableFuture().get());

    service.closeTask(taskId, USER);
    await().atMost(Duration.ofSeconds(5)).until(() -> closed.get() == 2);
  }

  @Test
  void failedDataProviderTask() throws ExecutionException, InterruptedException {

    var expectedResult = new StreamTaskResult() {};
    var completed = new AtomicInteger(0);
    var closed = new AtomicInteger(0);
    var descriptions =
        List.of(
            getDescription(expectedResult, completed, closed),
            getDescriptionWithFailedProvider(expectedResult, completed, closed),
            getDescription(null, completed, closed));

    var status =
        service
            .processTaskFlowAsTask(descriptions, TASK_TYPE, e -> {}, USER)
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
    await().atMost(Duration.ofSeconds(5)).until(() -> completed.get() == 2);

    assertThrows(
        ExecutionException.class,
        () -> service.getTaskResult(taskId, USER).toCompletableFuture().get());

    service.closeTask(taskId, USER);
    await().atMost(Duration.ofSeconds(5)).until(() -> closed.get() == 2);
  }

  @Test
  void failedAggregatorTask() throws ExecutionException, InterruptedException {

    var expectedResult = new StreamTaskResult() {};
    var completed = new AtomicInteger(0);
    var closed = new AtomicInteger(0);
    var descriptions =
        List.of(
            getDescription(expectedResult, completed, closed),
            getDescriptionWithFailedAggregator(expectedResult, completed, closed),
            getDescription(null, completed, closed));

    var status =
        service
            .processTaskFlowAsTask(descriptions, TASK_TYPE, e -> {}, USER)
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
    await().atMost(Duration.ofSeconds(5)).until(() -> completed.get() == 2);

    assertThrows(
        ExecutionException.class,
        () -> service.getTaskResult(taskId, USER).toCompletableFuture().get());

    service.closeTask(taskId, USER);
    await().atMost(Duration.ofSeconds(5)).until(() -> closed.get() == 2);
  }

  @Test
  void cancelFlow() throws ExecutionException, InterruptedException {

    var completed = new AtomicInteger(0);
    var closed = new AtomicInteger(0);
    var onComplete = new AtomicInteger(0);
    var taskIdRef = new AtomicReference<String>();

    var descriptions =
        List.of(
            getDescription(null, completed, closed),
            getDescription(null, completed, closed),
            getDescriptionWithFinalAction(
                completed,
                closed,
                () -> {
                  await()
                      .atMost(Duration.ofSeconds(5))
                      .until(() -> Objects.nonNull(taskIdRef.get()));
                  service.cancelTask(taskIdRef.get(), "");
                }));

    var status =
        service
            .processTaskFlowAsTask(descriptions, TASK_TYPE, e -> onComplete.incrementAndGet(), USER)
            .toCompletableFuture()
            .get();
    var taskId = status.taskId();
    taskIdRef.set(taskId);

    await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () -> {
              try {
                var statusCheck = service.getTaskStatus(taskId, USER).toCompletableFuture().get();
                return StreamTaskStatus.Status.COMPLETED.equals(statusCheck.status())
                    || StreamTaskStatus.Status.COMPLETED_WITH_ERROR.equals(statusCheck.status());
              } catch (Exception e) {
                return false;
              }
            });
    await().atMost(Duration.ofSeconds(50)).until(() -> completed.get() == 3);

    service.closeTask(taskId, USER);
    await().atMost(Duration.ofSeconds(5)).until(() -> closed.get() == 3);
    assertThat(onComplete.get()).isEqualTo(1);
  }

  private StreamTaskDescription getDescription(
      StreamTaskResult expectedResult, AtomicInteger completed, AtomicInteger closed) {
    var resultRef = new AtomicReference<>(expectedResult);
    return new StreamTaskDescription(
            () -> List.<DataProvider.Data>of(new WrapperData(resultRef.get())).iterator(),
            data -> data,
            new ResultAggregator() {
              DataProvider.Data consumedData;

              @Override
              public void consume(DataProvider.Data data) {
                consumedData = data;
              }

              @Override
              public StreamTaskResult getResult() {
                return ((WrapperData) consumedData).data();
              }

              @Override
              public double getPercent() {
                return 0.0;
              }

              @Override
              public void close() {
                closed.incrementAndGet();
              }

              @Override
              public void taskCompleted(boolean successful) {
                completed.incrementAndGet();
              }
            },
            USER,
            TASK_TYPE)
        .setWeight(1f)
        .setSubscription(resultRef::set)
        .setDetailsSupplier(() -> Map.of("detials", "test data"));
  }

  private StreamTaskDescription getDescriptionWithFinalAction(
      AtomicInteger completed, AtomicInteger closed, Runnable finalAction) {
    return new StreamTaskDescription(
        () -> List.<DataProvider.Data>of(new DataProvider.Data() {}).iterator(),
        data -> data,
        new ResultAggregator() {
          DataProvider.Data consumedData;

          @Override
          public void consume(DataProvider.Data data) {
            consumedData = data;
          }

          @Override
          public StreamTaskResult getResult() {
            return null;
          }

          @Override
          public double getPercent() {
            return 0.0;
          }

          @Override
          public void close() {
            closed.incrementAndGet();
          }

          @Override
          public void taskCompleted(boolean successful) {
            completed.incrementAndGet();
            finalAction.run();
          }
        },
        USER,
        TASK_TYPE);
  }

  private StreamTaskDescription getDescriptionWithFailedFunction(
      StreamTaskResult expectedResult, AtomicInteger completed, AtomicInteger closed) {
    var resultRef = new AtomicReference<>(expectedResult);
    return new StreamTaskDescription(
            () -> List.<DataProvider.Data>of(new WrapperData(resultRef.get())).iterator(),
            data -> {
              throw new RuntimeException("test exception");
            },
            new ResultAggregator() {
              DataProvider.Data consumedData;

              @Override
              public void consume(DataProvider.Data data) {
                consumedData = data;
              }

              @Override
              public StreamTaskResult getResult() {
                return consumedData == null ? null : ((WrapperData) consumedData).data();
              }

              @Override
              public double getPercent() {
                return 0.0;
              }

              @Override
              public void close() {
                closed.incrementAndGet();
              }

              @Override
              public void taskCompleted(boolean successful) {
                completed.incrementAndGet();
              }
            },
            USER,
            TASK_TYPE)
        .setSubscription(resultRef::set);
  }

  private StreamTaskDescription getDescriptionWithFailedProvider(
      StreamTaskResult expectedResult, AtomicInteger completed, AtomicInteger closed) {
    var resultRef = new AtomicReference<>(expectedResult);
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
              DataProvider.Data consumedData;

              @Override
              public void consume(DataProvider.Data data) {
                consumedData = data;
              }

              @Override
              public StreamTaskResult getResult() {
                return consumedData == null ? null : ((WrapperData) consumedData).data();
              }

              @Override
              public double getPercent() {
                return 0.0;
              }

              @Override
              public void close() {
                closed.incrementAndGet();
              }

              @Override
              public void taskCompleted(boolean successful) {
                completed.incrementAndGet();
              }
            },
            USER,
            TASK_TYPE)
        .setSubscription(resultRef::set);
  }

  private StreamTaskDescription getDescriptionWithFailedAggregator(
      StreamTaskResult expectedResult, AtomicInteger completed, AtomicInteger closed) {
    var resultRef = new AtomicReference<>(expectedResult);
    return new StreamTaskDescription(
            () -> List.<DataProvider.Data>of(new WrapperData(resultRef.get())).iterator(),
            data -> data,
            new ResultAggregator() {
              DataProvider.Data consumedData;

              @Override
              public void consume(DataProvider.Data data) {
                throw new RuntimeException("test error");
              }

              @Override
              public StreamTaskResult getResult() {
                return consumedData == null ? null : ((WrapperData) consumedData).data();
              }

              @Override
              public double getPercent() {
                return 0.0;
              }

              @Override
              public void close() {
                closed.incrementAndGet();
              }

              @Override
              public void taskCompleted(boolean successful) {
                completed.incrementAndGet();
              }
            },
            USER,
            TASK_TYPE)
        .setSubscription(resultRef::set);
  }

  record WrapperData(StreamTaskResult data) implements DataProvider.Data {}
}
