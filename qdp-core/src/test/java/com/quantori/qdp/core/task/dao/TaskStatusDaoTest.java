package com.quantori.qdp.core.task.dao;

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
import com.quantori.qdp.core.source.SourceRootActor;
import com.quantori.qdp.core.task.ContainerizedTest;
import com.quantori.qdp.core.task.model.StreamTaskStatus;
import com.quantori.qdp.core.task.model.TaskStatus;
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

    UUID taskId = UUID.randomUUID();
    StreamTaskStatus.Status status = StreamTaskStatus.Status.IN_PROGRESS;
    String type = "Upload";
    Date createdDate = new Date();
    Date updatedDate = new Date();
    String user = "user";
    String deserializer = "deserializer";
    String flowId = "flowId";
    int parallelism = 15;
    String state = "state";
    int restartFlag = 0;
    int buffer = 20;

    TaskStatus taskStatus = TaskStatus.builder()
        .taskId(taskId)
        .status(status)
        .type(type)
        .createdDate(createdDate)
        .updatedDate(updatedDate)
        .user(user)
        .deserializer(deserializer)
        .flowId(flowId)
        .parallelism(parallelism)
        .state(state)
        .restartFlag(restartFlag)
        .buffer(buffer)
        .build();
    dao.save(taskStatus);

    List<TaskStatus> all = dao.findAll();
    assertThat(all, is(notNullValue()));
    assertThat(all, hasSize(1));
    assertThat(all, containsInAnyOrder(taskStatus));
  }

  @Test
  void findByIdReturnsSavedTaskStatusIfPresent() {
    assertEmpty();

    UUID taskId = UUID.randomUUID();
    StreamTaskStatus.Status status = StreamTaskStatus.Status.IN_PROGRESS;
    String type = "Upload";
    Date createdDate = new Date();
    Date updatedDate = new Date();
    String user = "user";
    String deserializer = "deserializer";
    String flowId = "flowId";
    int parallelism = 15;
    String state = "state";
    int restartFlag = 0;
    int buffer = 20;

    TaskStatus expectedStatus = TaskStatus.builder()
        .taskId(taskId)
        .status(status)
        .type(type)
        .createdDate(createdDate)
        .updatedDate(updatedDate)
        .user(user)
        .deserializer(deserializer)
        .flowId(flowId)
        .parallelism(parallelism)
        .state(state)
        .restartFlag(restartFlag)
        .buffer(buffer)
        .build();
    dao.save(expectedStatus);

    Optional<TaskStatus> byId = dao.findById(taskId);

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

    UUID taskId1 = UUID.randomUUID();
    StreamTaskStatus.Status status1 = StreamTaskStatus.Status.IN_PROGRESS;
    String type1 = "Upload";
    Date createdDate1 = new Date();
    Date updatedDate1 = new Date();
    String user1 = "user";
    String deserializer1 = "deserializer";
    String flowId1 = "flowId";
    int parallelism1 = 15;
    String state1 = "state";
    int restartFlag1 = 0;
    int buffer1 = 20;

    TaskStatus expectedStatus1 = TaskStatus.builder()
        .taskId(taskId1)
        .status(status1)
        .type(type1)
        .createdDate(createdDate1)
        .updatedDate(updatedDate1)
        .user(user1)
        .deserializer(deserializer1)
        .flowId(flowId1)
        .parallelism(parallelism1)
        .state(state1)
        .restartFlag(restartFlag1)
        .buffer(buffer1)
        .build();

    UUID taskId2 = UUID.randomUUID();
    StreamTaskStatus.Status status2 = StreamTaskStatus.Status.IN_PROGRESS;
    String type2 = "Upload";
    Date createdDate2 = new Date();
    Date updatedDate2 = new Date();
    String user2 = "user";
    String deserializer2 = "deserializer";
    String flowId2 = "flowId";
    int parallelism2 = 15;
    String state2 = "state";
    int restartFlag2 = 0;
    int buffer2 = 20;

    TaskStatus expectedStatus2 = TaskStatus.builder()
        .taskId(taskId2)
        .status(status2)
        .type(type2)
        .createdDate(createdDate2)
        .updatedDate(updatedDate2)
        .user(user2)
        .deserializer(deserializer2)
        .flowId(flowId2)
        .parallelism(parallelism2)
        .state(state2)
        .restartFlag(restartFlag2)
        .buffer(buffer2)
        .build();

    UUID taskId3 = UUID.randomUUID();
    StreamTaskStatus.Status status3 = StreamTaskStatus.Status.IN_PROGRESS;
    String type3 = "Upload";
    Date createdDate3 = new Date();
    Date updatedDate3 = new Date();
    String user3 = "user";
    String deserializer3 = "deserializer";
    String flowId3 = "flowId";
    int parallelism3 = 15;
    String state3 = "state";
    int restartFlag3 = 0;
    int buffer3 = 20;

    TaskStatus expectedStatus3 = TaskStatus.builder()
        .taskId(taskId3)
        .status(status3)
        .type(type3)
        .createdDate(createdDate3)
        .updatedDate(updatedDate3)
        .user(user3)
        .deserializer(deserializer3)
        .flowId(flowId3)
        .parallelism(parallelism3)
        .state(state3)
        .restartFlag(restartFlag3)
        .buffer(buffer3)
        .build();

    dao.save(expectedStatus1);
    dao.save(expectedStatus2);
    dao.save(expectedStatus3);

    List<TaskStatus> actualStatuses = dao.findAll();

    assertThat(actualStatuses, is(notNullValue()));
    assertThat(actualStatuses, hasSize(3));
    assertThat(actualStatuses, containsInAnyOrder(expectedStatus1, expectedStatus2, expectedStatus3));
  }

  @Test
  void deleteRemovesSpecificTaskStatus() {
    assertEmpty();

    UUID taskId = UUID.randomUUID();
    StreamTaskStatus.Status status = StreamTaskStatus.Status.IN_PROGRESS;
    String type = "Upload";
    Date createdDate = new Date();
    Date updatedDate = new Date();
    String user = "user";
    String deserializer = "deserializer";
    String flowId = "flowId";
    int parallelism = 15;
    String state = "state";
    int restartFlag = 0;
    int buffer = 20;

    TaskStatus expectedStatus = TaskStatus.builder()
        .taskId(taskId)
        .status(status)
        .type(type)
        .createdDate(createdDate)
        .updatedDate(updatedDate)
        .user(user)
        .deserializer(deserializer)
        .flowId(flowId)
        .parallelism(parallelism)
        .state(state)
        .restartFlag(restartFlag)
        .buffer(buffer)
        .build();

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

    UUID taskId1 = UUID.randomUUID();
    StreamTaskStatus.Status status1 = StreamTaskStatus.Status.IN_PROGRESS;
    String type1 = "Upload";
    Date createdDate1 = new Date();
    Date updatedDate1 = new Date();
    String user1 = "user";
    String deserializer1 = "deserializer";
    String flowId1 = "flowId";
    int parallelism1 = 15;
    String state1 = "state";
    int restartFlag1 = 0;
    int buffer1 = 20;

    TaskStatus expectedStatus1 = TaskStatus.builder()
        .taskId(taskId1)
        .status(status1)
        .type(type1)
        .createdDate(createdDate1)
        .updatedDate(updatedDate1)
        .user(user1)
        .deserializer(deserializer1)
        .flowId(flowId1)
        .parallelism(parallelism1)
        .state(state1)
        .restartFlag(restartFlag1)
        .buffer(buffer1)
        .build();

    UUID taskId2 = UUID.randomUUID();
    StreamTaskStatus.Status status2 = StreamTaskStatus.Status.IN_PROGRESS;
    String type2 = "Upload";
    Date createdDate2 = new Date();
    Date updatedDate2 = new Date();
    String user2 = "user";
    String deserializer2 = "deserializer";
    String flowId2 = "flowId";
    int parallelism2 = 15;
    String state2 = "state";
    int restartFlag2 = 0;
    int buffer2 = 20;

    TaskStatus expectedStatus2 = TaskStatus.builder()
        .taskId(taskId2)
        .status(status2)
        .type(type2)
        .createdDate(createdDate2)
        .updatedDate(updatedDate2)
        .user(user2)
        .deserializer(deserializer2)
        .flowId(flowId2)
        .parallelism(parallelism2)
        .state(state2)
        .restartFlag(restartFlag2)
        .buffer(buffer2)
        .build();

    UUID taskId3 = UUID.randomUUID();
    StreamTaskStatus.Status status3 = StreamTaskStatus.Status.IN_PROGRESS;
    String type3 = "Upload";
    Date createdDate3 = new Date();
    Date updatedDate3 = new Date();
    String user3 = "user";
    String deserializer3 = "deserializer";
    String flowId3 = "flowId";
    int parallelism3 = 15;
    String state3 = "state";
    int restartFlag3 = 0;
    int buffer3 = 20;

    TaskStatus expectedStatus3 = TaskStatus.builder()
        .taskId(taskId3)
        .status(status3)
        .type(type3)
        .createdDate(createdDate3)
        .updatedDate(updatedDate3)
        .user(user3)
        .deserializer(deserializer3)
        .flowId(flowId3)
        .parallelism(parallelism3)
        .state(state3)
        .restartFlag(restartFlag3)
        .buffer(buffer3)
        .build();

    dao.save(expectedStatus1);
    dao.save(expectedStatus2);
    dao.save(expectedStatus3);

    List<TaskStatus> allById =
        dao.findAllById(List.of(expectedStatus1.getTaskId(), expectedStatus2.getTaskId(), expectedStatus3.getTaskId()));

    assertThat(allById, is(notNullValue()));
    assertThat(allById, hasSize(3));
    assertThat(allById, containsInAnyOrder(expectedStatus1, expectedStatus2, expectedStatus3));
  }

  @Test
  void findTaskStatusWithPessimisticLockReturnsSavedTaskStatusIfPresent() {
    assertEmpty();

    UUID taskId = UUID.randomUUID();
    StreamTaskStatus.Status status = StreamTaskStatus.Status.IN_PROGRESS;
    String type = "Upload";
    Date createdDate = new Date();
    Date updatedDate = new Date();
    String user = "user";
    String deserializer = "deserializer";
    String flowId = "flowId";
    int parallelism = 15;
    String state = "state";
    int restartFlag = 0;
    int buffer = 20;

    TaskStatus expectedStatus = TaskStatus.builder()
        .taskId(taskId)
        .status(status)
        .type(type)
        .createdDate(createdDate)
        .updatedDate(updatedDate)
        .user(user)
        .deserializer(deserializer)
        .flowId(flowId)
        .parallelism(parallelism)
        .state(state)
        .restartFlag(restartFlag)
        .buffer(buffer)
        .build();
    dao.save(expectedStatus);

    Optional<TaskStatus> byId = dao.findById(taskId);

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

    UUID taskId = UUID.randomUUID();
    StreamTaskStatus.Status status = StreamTaskStatus.Status.IN_PROGRESS;
    String type = "Upload";
    String user = "user";
    String deserializer = "deserializer";
    String flowId = "flowId";
    int parallelism = 15;
    String state = "state";
    int restartFlag = 0;
    int buffer = 20;

    TaskStatus expectedStatus = TaskStatus.builder()
        .taskId(taskId)
        .status(status)
        .type(type)
        .user(user)
        .deserializer(deserializer)
        .flowId(flowId)
        .parallelism(parallelism)
        .state(state)
        .restartFlag(restartFlag)
        .buffer(buffer)
        .build();

    Instant beforeSave = Instant.now();
    dao.save(expectedStatus);
    Instant afterSave = Instant.now();

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
    String originalType = "Upload";
    Date originalCreatedDate = new Date();
    Date originalUpdatedDate = new Date();
    String originalUser = "originalUser";
    String originalDeserializer = "originalDerializer";
    String originalFlowId = "originalFlowId";
    int originalParallelism = 15;
    String originalState = "originalState";
    int originalRestartFlag = 0;
    int originalBuffer = 20;

    TaskStatus originalTaskStatus = TaskStatus.builder()
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

    TaskStatus updatedTaskStatus = TaskStatus.builder()
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
    assertThat(actualTaskStatus.getUpdatedDate().toInstant(), is(greaterThanOrEqualTo(beforeUpdate)));
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

    UUID taskId1 = UUID.randomUUID();
    StreamTaskStatus.Status status1 = StreamTaskStatus.Status.IN_PROGRESS;
    String type1 = "Upload";
    Date createdDate1 = new Date();
    Date updatedDate1 = new Date();
    String user1 = "user";
    String deserializer1 = "deserializer";
    String flowId1 = "flowId";
    int parallelism1 = 15;
    String state1 = "state";
    int restartFlag1 = 0;
    int buffer1 = 20;

    TaskStatus expectedStatus1 = TaskStatus.builder()
        .taskId(taskId1)
        .status(status1)
        .type(type1)
        .createdDate(createdDate1)
        .updatedDate(updatedDate1)
        .user(user1)
        .deserializer(deserializer1)
        .flowId(flowId1)
        .parallelism(parallelism1)
        .state(state1)
        .restartFlag(restartFlag1)
        .buffer(buffer1)
        .build();

    UUID taskId2 = UUID.randomUUID();
    StreamTaskStatus.Status status2 = StreamTaskStatus.Status.IN_PROGRESS;
    String type2 = "Upload";
    Date createdDate2 = new Date();
    Date updatedDate2 = new Date();
    String user2 = "user";
    String deserializer2 = "deserializer";
    String flowId2 = "flowId";
    int parallelism2 = 15;
    String state2 = "state";
    int restartFlag2 = 0;
    int buffer2 = 20;

    TaskStatus expectedStatus2 = TaskStatus.builder()
        .taskId(taskId2)
        .status(status2)
        .type(type2)
        .createdDate(createdDate2)
        .updatedDate(updatedDate2)
        .user(user2)
        .deserializer(deserializer2)
        .flowId(flowId2)
        .parallelism(parallelism2)
        .state(state2)
        .restartFlag(restartFlag2)
        .buffer(buffer2)
        .build();

    UUID taskId3 = UUID.randomUUID();
    StreamTaskStatus.Status status3 = StreamTaskStatus.Status.IN_PROGRESS;
    String type3 = "Upload";
    Date createdDate3 = new Date();
    Date updatedDate3 = new Date();
    String user3 = "user";
    String deserializer3 = "deserializer";
    String flowId3 = "flowId";
    int parallelism3 = 15;
    String state3 = "state";
    int restartFlag3 = 0;
    int buffer3 = 20;

    TaskStatus expectedStatus3 = TaskStatus.builder()
        .taskId(taskId3)
        .status(status3)
        .type(type3)
        .createdDate(createdDate3)
        .updatedDate(updatedDate3)
        .user(user3)
        .deserializer(deserializer3)
        .flowId(flowId3)
        .parallelism(parallelism3)
        .state(state3)
        .restartFlag(restartFlag3)
        .buffer(buffer3)
        .build();

    dao.save(expectedStatus1);
    dao.save(expectedStatus2);
    dao.save(expectedStatus3);

    List<TaskStatus> savedStatuses = dao.findAll();

    assertThat(savedStatuses, is(notNullValue()));
    assertThat(savedStatuses, hasSize(3));
    assertThat(savedStatuses, containsInAnyOrder(expectedStatus1, expectedStatus2, expectedStatus3));

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
    assertThat(all, hasSize(0));
  }
}