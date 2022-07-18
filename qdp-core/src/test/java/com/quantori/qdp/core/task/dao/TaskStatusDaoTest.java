package com.quantori.qdp.core.task.dao;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;

import akka.actor.typed.ActorSystem;
import akka.stream.alpakka.slick.javadsl.SlickSession;
import akka.stream.alpakka.slick.javadsl.SlickSession$;
import com.quantori.qdp.core.source.MoleculeSourceRootActor;
import com.quantori.qdp.core.task.model.StreamTaskDetails;
import com.quantori.qdp.core.task.model.StreamTaskStatus;
import com.quantori.qdp.core.task.model.TaskStatus;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TaskStatusDaoTest extends ContainerizedTest {

  private static ActorSystem<MoleculeSourceRootActor.Command> actorSystem;
  private static TaskStatusDao dao;

  @BeforeAll
  static void setUp() {
    actorSystem = ActorSystem.create(MoleculeSourceRootActor.create(100), "test-actor-system");
    SlickSession session = SlickSession$.MODULE$.forConfig(getSlickConfig());
    actorSystem.classicSystem().registerOnTermination(session::close);
    dao = new TaskStatusDao(session, actorSystem);
  }

  @Test
  void test() {
    List<TaskStatus> all = dao.findAll();

    assertThat(all, notNullValue());
    assertThat(all, hasSize(0));
  }

  @Test
  void test1() {
    UUID taskId = UUID.randomUUID();
    StreamTaskStatus.Status status = StreamTaskStatus.Status.IN_PROGRESS;
    StreamTaskDetails.TaskType type = StreamTaskDetails.TaskType.Upload;
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
  void test2() {
    List<TaskStatus> all = dao.findAll();

    assertThat(all, is(notNullValue()));
    assertThat(all, hasSize(0));

    UUID taskId = UUID.randomUUID();
    StreamTaskStatus.Status status = StreamTaskStatus.Status.IN_PROGRESS;
    StreamTaskDetails.TaskType type = StreamTaskDetails.TaskType.Upload;
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

  private static Config getSlickConfig() {
    HashMap<String, String> map = new HashMap<>();
    map.put("profile", "slick.jdbc.PostgresProfile$");
    map.put("db.dataSourceClass", "slick.jdbc.DriverDataSource");
    map.put("db.properties.driver", "org.postgresql.Driver");
    map.put("db.properties.url", getDBUrlString());
    map.put("db.properties.user", getDBUserName());
    map.put("db.properties.password", getDBPassword());

    return ConfigFactory.parseMap(map);
  }

  @NotNull
  private static String getDBUrlString() {
    return postgresql.getJdbcUrl();
  }

  @NotNull
  private static String getDBUserName() {
    return postgresql.getUsername();
  }

  @NotNull
  private static String getDBPassword() {
    return postgresql.getPassword();
  }

  @AfterEach
  void clearTable() throws IOException, InterruptedException {
    postgresql.execInContainer("psql",
        "-U", getDBUserName(),
        "-d", postgresql.getDatabaseName(),
        "-f", "/initdb.sql");
  }

  @AfterAll
  static void shutDown() {
    actorSystem.terminate();
  }
}