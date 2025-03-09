package com.quantori.cqp.core.task.dao;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;

import akka.actor.typed.ActorSystem;
import akka.stream.alpakka.slick.javadsl.SlickSession;
import akka.stream.alpakka.slick.javadsl.SlickSession$;
import com.quantori.cqp.core.source.SourceRootActor;
import com.quantori.cqp.core.task.ContainerizedTest;
import com.quantori.cqp.core.task.model.StreamTaskStatus;
import com.quantori.cqp.core.task.model.TaskStatus;
import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TaskStatusDaoTest extends ContainerizedTest {

  private static final String USER = "user";
  private static final String TYPE = "Type";
  private static final String DESERIALIZER = "deserializer";
  private static final String FLOW_ID = "flowId";
  private static final String STATE = "state";
  private static final int PARALLELISM = 15;
  private static final int RESTART_FLAG = 0;
  private static final int BUFFER = 20;
  private static ActorSystem<SourceRootActor.Command> actorSystem;
  private static TaskStatusDao dao;

  @BeforeAll
  static void setUp() {
    actorSystem = ActorSystem.create(SourceRootActor.create(100), "test-actor-system");
    SlickSession session = SlickSession$.MODULE$.forConfig(getSlickConfig());
    actorSystem.classicSystem().registerOnTermination(session::close);
    dao = new TaskStatusDao(session, actorSystem);
  }

  @Test
  void findAllReturnsEmptyListOnEmptyTable() {
    assertEmpty();
  }

  @Test
  void savesTaskStatusCorrectly() {
    assertEmpty();

    TaskStatus taskStatus = buildStatus();
    dao.save(taskStatus);

    List<TaskStatus> all = dao.findAll();
    assertThat(all, is(notNullValue()));
    assertThat(all, hasSize(1));
    assertThat(all, containsInAnyOrder(taskStatus));
  }

  @Test
  void findByIdReturnsSavedTaskStatusIfPresent() {
    assertEmpty();

    TaskStatus expectedStatus = buildStatus();
    dao.save(expectedStatus);

    Optional<TaskStatus> byId = dao.findById(expectedStatus.getTaskId());

    TaskStatus actualStatus = byId.get();

    assertThat(actualStatus, is(equalTo(expectedStatus)));
  }

  @Test
  void findByIdReturnsEmptyOptionalIfNotPresent() {
    assertEmpty();

    Optional<TaskStatus> byId = dao.findById(UUID.randomUUID());

    assertThat(byId, is(notNullValue()));
    assertThat(byId, is(Optional.empty()));
  }

  @Test
  void findAllReturnsListOfAllSavedTaskStatuses() {
    assertEmpty();

    TaskStatus expectedStatus1 = buildStatus();
    TaskStatus expectedStatus2 = buildStatus();
    TaskStatus expectedStatus3 = buildStatus();

    dao.save(expectedStatus1);
    dao.save(expectedStatus2);
    dao.save(expectedStatus3);

    List<TaskStatus> actualStatuses = dao.findAll();

    assertThat(actualStatuses, is(notNullValue()));
    assertThat(actualStatuses, hasSize(3));
    assertThat(
        actualStatuses, containsInAnyOrder(expectedStatus1, expectedStatus2, expectedStatus3));
  }

  @Test
  void deleteRemovesSpecificTaskStatus() {
    assertEmpty();

    TaskStatus expectedStatus = buildStatus();
    dao.save(expectedStatus);

    List<TaskStatus> all = dao.findAll();

    assertThat(all, is(notNullValue()));
    assertThat(all, hasSize(1));
    assertThat(all, containsInAnyOrder(expectedStatus));

    dao.delete(expectedStatus);

    assertEmpty();
  }

  @Test
  void findAllByIdReturnsListOfTaskStatusesWithSpecificId() {
    assertEmpty();

    TaskStatus expectedStatus1 = buildStatus();
    TaskStatus expectedStatus2 = buildStatus();
    TaskStatus expectedStatus3 = buildStatus();

    dao.save(expectedStatus1);
    dao.save(expectedStatus2);
    dao.save(expectedStatus3);

    List<TaskStatus> allById =
        dao.findAllById(
            List.of(
                expectedStatus1.getTaskId(),
                expectedStatus2.getTaskId(),
                expectedStatus3.getTaskId()));

    assertThat(allById, is(notNullValue()));
    assertThat(allById, hasSize(3));
    assertThat(allById, containsInAnyOrder(expectedStatus1, expectedStatus2, expectedStatus3));
  }

  @Test
  void findTaskStatusWithPessimisticLockReturnsSavedTaskStatusIfPresent() {
    assertEmpty();

    TaskStatus expectedStatus = buildStatus();
    dao.save(expectedStatus);

    Optional<TaskStatus> byId = dao.findById(expectedStatus.getTaskId());

    TaskStatus actualStatus = byId.get();

    assertThat(actualStatus, is(equalTo(expectedStatus)));
  }

  @Test
  void findTaskStatusWithPessimisticLockReturnsEmptyOptionalIfNotPresent() {
    assertEmpty();

    Optional<TaskStatus> byId = dao.findTaskStatusWithPessimisticLock(UUID.randomUUID());

    assertThat(byId, is(notNullValue()));
    assertThat(byId, is(Optional.empty()));
  }

  @Test
  void saveSetsCurrentTimestampAsCreatedAndUpdatedTimeIfNotSpecified() {
    assertEmpty();

    Instant beforeSave = new Date().toInstant();
    TaskStatus expectedStatus = buildStatus();
    dao.save(expectedStatus);
    Instant afterSave = new Date().toInstant();

    Optional<TaskStatus> byId = dao.findById(expectedStatus.getTaskId());
    TaskStatus actualStatus = byId.get();

    assertThat(actualStatus.getCreatedDate().toInstant(), is(greaterThanOrEqualTo(beforeSave)));
    assertThat(actualStatus.getCreatedDate().toInstant(), is(lessThanOrEqualTo(afterSave)));
    assertThat(actualStatus.getUpdatedDate().toInstant(), is(greaterThanOrEqualTo(beforeSave)));
    assertThat(actualStatus.getUpdatedDate().toInstant(), is(lessThanOrEqualTo(afterSave)));
  }

  @Test
  void saveUpdatesPropertiesButKeepsOriginalCreatedDateWhenTaskStatusPresent() {
    assertEmpty();

    UUID originalTaskId = UUID.randomUUID();
    StreamTaskStatus.Status originalStatus = StreamTaskStatus.Status.IN_PROGRESS;
    String originalType = "originalType";
    Date originalCreatedDate = new Date();
    Date originalUpdatedDate = new Date();
    String originalUser = "originalUser";
    String originalDeserializer = "originalDerializer";
    String originalFlowId = "originalFlowId";
    int originalParallelism = 15;
    String originalState = "originalState";
    int originalRestartFlag = 0;
    int originalBuffer = 20;

    TaskStatus originalTaskStatus =
        TaskStatus.builder()
            .taskId(originalTaskId)
            .status(originalStatus)
            .type(originalType)
            .createdDate(originalCreatedDate)
            .updatedDate(originalUpdatedDate)
            .user(originalUser)
            .deserializer(originalDeserializer)
            .flowId(originalFlowId)
            .parallelism(originalParallelism)
            .state(originalState)
            .restartFlag(originalRestartFlag)
            .buffer(originalBuffer)
            .build();

    dao.save(originalTaskStatus);

    Optional<TaskStatus> byId = dao.findById(originalTaskId);
    TaskStatus savedTaskStatus = byId.get();

    assertThat(savedTaskStatus, is(equalTo(originalTaskStatus)));

    StreamTaskStatus.Status updatedStatus = StreamTaskStatus.Status.COMPLETED;
    String updatedType = "Merge";
    String updatedUser = "updatedUser";
    String updatedDeserializer = "updatedDeserializer";
    String updatedFlowId = "updatedFlowId";
    int updatedParallelism = 20;
    String updatedState = "updatedState";
    int updatedRestartFlag = 1;
    int updatedBuffer = 30;

    TaskStatus updatedTaskStatus =
        TaskStatus.builder()
            .taskId(originalTaskId)
            .status(updatedStatus)
            .type(updatedType)
            .user(updatedUser)
            .deserializer(updatedDeserializer)
            .flowId(updatedFlowId)
            .parallelism(updatedParallelism)
            .state(updatedState)
            .restartFlag(updatedRestartFlag)
            .buffer(updatedBuffer)
            .build();

    Instant beforeUpdate = Instant.now();
    dao.save(updatedTaskStatus);
    Instant afterUpdate = Instant.now();

    TaskStatus actualTaskStatus = dao.findById(originalTaskId).get();

    assertThat(actualTaskStatus.getTaskId(), is(equalTo(originalTaskId)));
    assertThat(actualTaskStatus.getStatus(), is(equalTo(updatedStatus)));
    assertThat(actualTaskStatus.getType(), is(equalTo(updatedType)));
    assertThat(actualTaskStatus.getCreatedDate(), is(equalTo(originalCreatedDate)));
    assertThat(
        actualTaskStatus.getUpdatedDate().toInstant(), is(greaterThanOrEqualTo(beforeUpdate)));
    assertThat(actualTaskStatus.getUpdatedDate().toInstant(), is(lessThanOrEqualTo(afterUpdate)));
    assertThat(actualTaskStatus.getUser(), is(equalTo(updatedUser)));
    assertThat(actualTaskStatus.getDeserializer(), is(equalTo(updatedDeserializer)));
    assertThat(actualTaskStatus.getFlowId(), is(equalTo(updatedFlowId)));
    assertThat(actualTaskStatus.getParallelism(), is(equalTo(updatedParallelism)));
    assertThat(actualTaskStatus.getState(), is(equalTo(updatedState)));
    assertThat(actualTaskStatus.getRestartFlag(), is(equalTo(updatedRestartFlag)));
    assertThat(actualTaskStatus.getBuffer(), is(equalTo(updatedBuffer)));
  }

  @Test
  void deleteAll() {
    assertEmpty();

    TaskStatus expectedStatus1 = buildStatus();
    TaskStatus expectedStatus2 = buildStatus();
    TaskStatus expectedStatus3 = buildStatus();

    dao.save(expectedStatus1);
    dao.save(expectedStatus2);
    dao.save(expectedStatus3);

    List<TaskStatus> savedStatuses = dao.findAll();

    assertThat(savedStatuses, is(notNullValue()));
    assertThat(savedStatuses, hasSize(3));
    assertThat(
        savedStatuses, containsInAnyOrder(expectedStatus1, expectedStatus2, expectedStatus3));

    dao.deleteAll();

    assertEmpty();
  }

  @AfterEach
  void clear() throws IOException, InterruptedException {
    reinitTable();
  }

  @AfterAll
  static void shutDown() {
    actorSystem.terminate();
  }

  private void assertEmpty() {
    List<TaskStatus> all = dao.findAll();

    assertThat(all, is(notNullValue()));
    assertThat(all, hasSize(RESTART_FLAG));
  }

  private TaskStatus buildStatus() {
    UUID taskId = UUID.randomUUID();
    Date createdDate = new Date();
    Date updatedDate = new Date();

    return TaskStatus.builder()
        .taskId(taskId)
        .status(StreamTaskStatus.Status.IN_PROGRESS)
        .type(TYPE)
        .createdDate(createdDate)
        .updatedDate(updatedDate)
        .user(USER)
        .deserializer(DESERIALIZER)
        .flowId(FLOW_ID)
        .parallelism(PARALLELISM)
        .state(STATE)
        .restartFlag(RESTART_FLAG)
        .buffer(BUFFER)
        .build();
  }
}
