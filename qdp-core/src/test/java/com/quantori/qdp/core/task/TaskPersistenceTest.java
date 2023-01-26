package com.quantori.qdp.core.task;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.stream.alpakka.slick.javadsl.SlickSession;
import akka.stream.alpakka.slick.javadsl.SlickSession$;
import com.quantori.qdp.core.source.SourceRootActor;
import com.quantori.qdp.core.task.dao.TaskStatusDao;
import com.quantori.qdp.core.task.model.DataProvider;
import com.quantori.qdp.core.task.model.DescriptionState;
import com.quantori.qdp.core.task.model.ResultAggregator;
import com.quantori.qdp.core.task.model.ResumableTaskDescription;
import com.quantori.qdp.core.task.model.StreamTaskDescription;
import com.quantori.qdp.core.task.model.StreamTaskResult;
import com.quantori.qdp.core.task.model.StreamTaskStatus;
import com.quantori.qdp.core.task.model.TaskDescriptionSerDe;
import com.quantori.qdp.core.task.service.StreamTaskService;
import com.quantori.qdp.core.task.service.StreamTaskServiceImpl;
import com.quantori.qdp.core.task.service.TaskPersistenceService;
import com.quantori.qdp.core.task.service.TaskPersistenceServiceImpl;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
class TaskPersistenceTest extends ContainerizedTest {
  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static StreamTaskService service;
  private static TaskPersistenceService persistenceService;
  private static TaskStatusDao taskStatusDao;
  private static ActorTestKit actorTestKit;
  private static ActorSystem<SourceRootActor.Command> system;

  @BeforeAll
  static void setup() {
    system =
        ActorSystem.create(SourceRootActor.create(100), "test-actor-system");
    actorTestKit = ActorTestKit.create(system);
    SlickSession session = SlickSession$.MODULE$.forConfig(getSlickConfig());
    system.classicSystem().registerOnTermination(session::close);
    taskStatusDao = new TaskStatusDao(session, system);

    Behavior<TaskServiceActor.Command> commandBehavior = TaskServiceActor.create();

    ActorRef<TaskServiceActor.Command> commandActorRef = actorTestKit.spawn(commandBehavior);
    service = new StreamTaskServiceImpl(system, commandActorRef, () -> persistenceService);
    persistenceService =
        new TaskPersistenceServiceImpl(system, commandActorRef, () -> service, taskStatusDao, new Object(), false);
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
  void taskPersistence() throws ExecutionException, InterruptedException {
    Assertions.assertTrue(taskStatusDao.findAll().isEmpty());
    //start task
    var status = service.processTask(getDescription(), null).toCompletableFuture().get();
    assertThat(status.status()).isEqualTo(StreamTaskStatus.Status.IN_PROGRESS);

    //get in progress status
    await().atMost(Duration.ofSeconds(5)).until(() ->
        service.getTaskStatus(status.taskId(), "user").toCompletableFuture().get().status()
            .equals(StreamTaskStatus.Status.IN_PROGRESS));

    //close task
    service.closeTask(status.taskId(), "user");

    //get not exist status
    await().atMost(Duration.ofSeconds(5)).until(() ->
        persistenceService.taskActorDoesNotExists(UUID.fromString(status.taskId())));

    //check persistence
    var task = taskStatusDao.findById(UUID.fromString(status.taskId()));
    Assertions.assertTrue(task.isPresent());
    assertThat(task.get().getStatus()).isEqualTo(StreamTaskStatus.Status.IN_PROGRESS);
    assertThat(task.get().getRestartFlag()).isZero();

    //resume task execution
    persistenceService.resumeTask(UUID.fromString(status.taskId()), null);

    //get completed status
    await().atMost(Duration.ofSeconds(5)).until(() ->
        service.getTaskStatus(status.taskId(), "user").toCompletableFuture().get().status()
            .equals(StreamTaskStatus.Status.COMPLETED));

    //get task result
    var result = service.getTaskResult(status.taskId(), "user").toCompletableFuture().get().toString();
    assertThat(result).contains("initial processing").contains("final processing");

    //get not exist actor
    service.closeTask(status.taskId(), "user");
    await().atMost(Duration.ofSeconds(5)).until(() ->
        persistenceService.taskActorDoesNotExists(UUID.fromString(status.taskId())));

    Thread.sleep(Duration.ofSeconds(90).toMillis());
    persistenceService.restartInProgressTasks();
    Assertions.assertTrue(taskStatusDao.findAll().isEmpty());
  }

  @Test
  void persistCurrentlyRunning() throws ExecutionException, InterruptedException {
    Assertions.assertTrue(taskStatusDao.findAll().isEmpty());
    //start task
    var status = service.processTask(getDescription(), null).toCompletableFuture().get();
    assertThat(status.status()).isEqualTo(StreamTaskStatus.Status.IN_PROGRESS);

    //get in progress status
    await().atMost(Duration.ofSeconds(5)).until(() ->
        service.getTaskStatus(status.taskId(), "user").toCompletableFuture().get().status()
            .equals(StreamTaskStatus.Status.IN_PROGRESS));

    //resume task execution
    assertThatThrownBy(() ->
        persistenceService.resumeTask(UUID.fromString(status.taskId()), null).toCompletableFuture().get());

    //check the task is running
    var task = taskStatusDao.findById(UUID.fromString(status.taskId()));
    Assertions.assertTrue(task.isPresent());
    assertThat(task.get().getStatus()).isEqualTo(StreamTaskStatus.Status.IN_PROGRESS);
    assertThat(task.get().getRestartFlag()).isZero();

    //close task
    service.closeTask(status.taskId(), "user");

    //get not exist status
    await().atMost(Duration.ofSeconds(5)).until(() ->
        persistenceService.taskActorDoesNotExists(UUID.fromString(status.taskId())));

    //mark the task as completed in persistence
    taskStatusDao.save(
        taskStatusDao.findById(UUID.fromString(status.taskId())).get().setStatus(StreamTaskStatus.Status.COMPLETED));

    Thread.sleep(Duration.ofSeconds(120).toMillis());
    persistenceService.restartInProgressTasks();
    Assertions.assertTrue(taskStatusDao.findAll().isEmpty());
  }

  @Test
  void persistAlreadyFinished() throws ExecutionException, InterruptedException {
    Assertions.assertTrue(taskStatusDao.findAll().isEmpty());

    //start task
    var status = service.processTask(getFastDescription(), null).toCompletableFuture().get();
    assertThat(status.status()).isEqualTo(StreamTaskStatus.Status.IN_PROGRESS);


    //get completed status
    await().atMost(Duration.ofSeconds(5)).until(() ->
        service.getTaskStatus(status.taskId(), "user").toCompletableFuture().get().status()
            .equals(StreamTaskStatus.Status.COMPLETED));

    //task state info was persisted
    Assertions.assertEquals(1, taskStatusDao.findAll().size());

    //resume task execution
    assertThatThrownBy(() ->
        persistenceService.resumeTask(UUID.fromString(status.taskId()), null).toCompletableFuture().get());


    var task = taskStatusDao.findById(UUID.fromString(status.taskId()));
    Assertions.assertTrue(task.isPresent());
    assertThat(task.get().getStatus()).isEqualTo(StreamTaskStatus.Status.COMPLETED);
    assertThat(task.get().getRestartFlag()).isZero();

    //close task
    service.closeTask(status.taskId(), "user");

    //get not exist status
    await().atMost(Duration.ofSeconds(5)).until(() ->
        persistenceService.taskActorDoesNotExists(UUID.fromString(status.taskId())));

    Thread.sleep(Duration.ofSeconds(90).toMillis());
    persistenceService.restartInProgressTasks();
    Assertions.assertTrue(taskStatusDao.findAll().isEmpty());
  }

  ResumableTaskDescription getDescription() {
    return new ResumableTaskDescription(
        getDataProvider(),
        data -> new DataProvider.Data() {
        },
        new Aggregator("initial processing"),
        new SerDe(),
        "user",
        "BulkEdit"
    ) {
      @Override
      public DescriptionState getState() {
        return null;
      }
    };
  }


  ResumableTaskDescription getFastDescription() {
    return new ResumableTaskDescription(
        () -> List.<DataProvider.Data>of(new DataProvider.Data() {
        }).iterator(),
        data -> new DataProvider.Data() {
        },
        new Aggregator("some processing"),
        new SerDe(),
        "user",
        "BulkEdit"
    ) {
      @Override
      public DescriptionState getState() {
        return null;
      }
    };
  }

  public static class SerDe implements TaskDescriptionSerDe {

    @Override
    public StreamTaskDescription deserialize(String json) {
      return new ResumableTaskDescription(
          () -> List.<DataProvider.Data>of(new DataProvider.Data() {
          }).iterator(),
          data -> new DataProvider.Data() {
          },
          new Aggregator(json),
          new SerDe(),
          "user",
          "BulkEdit"
      ) {
        @Override
        public DescriptionState getState() {
          return null;
        }
      };
    }

    @Override
    public String serialize(DescriptionState state) {
      return "final processing";
    }

    @Override
    public void setRequiredEntities(Object entityHolder) {

    }
  }

  //    @NonNull
  private DataProvider getDataProvider() {
    return new DataProvider() {
      volatile boolean stop = false;

      @Override
      public Iterator<Data> dataIterator() {
        return new Iterator<>() {
          @Override
          public boolean hasNext() {
            return !stop;
          }

          @Override
          public Data next() {
            return new Data() {
            };
          }
        };
      }

      @Override
      public void close() {
        stop = true;
      }
    };
  }

  public static class Aggregator implements ResultAggregator {
    static StringBuffer result = new StringBuffer();
    String data;

    public Aggregator(String data) {
      this.data = data;
    }

    public void consume(DataProvider.Data item) {
      result.append(data).append("\n");
      try {
        Thread.sleep(100);
      } catch (Exception e) {
        logger.error("error: ", e);
      }
    }

    @Override
    public StreamTaskResult getResult() {
      return new StreamTaskResult() {

        @Override
        public String toString() {
          return result.toString();
        }
      };
    }

    @Override
    public double getPercent() {
      return 0.0;
    }
  }
}
