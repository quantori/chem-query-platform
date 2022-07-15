package com.quantori.qdp.core.task.dao;

import static org.hamcrest.MatcherAssert.assertThat;
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;

//@Testcontainers
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
    assertThat(all, is(notNullValue()));
    assertThat(all, hasSize(0));
    System.out.println("it's running");
  }

  @Test
  void test1() {
    UUID taskId = UUID.randomUUID();

    TaskStatus taskStatus = TaskStatus.builder()
        .taskId(taskId)
        .status(StreamTaskStatus.Status.IN_PROGRESS)
        .type(StreamTaskDetails.TaskType.Upload)
        .createdDate(new Date())
        .updatedDate(new Date())
        .user("user")
        .build();
    dao.save(taskStatus);

    List<TaskStatus> all = dao.findAll();
    assertThat(all, is(notNullValue()));
    assertThat(all, hasSize(1));

    System.out.println(taskStatus);
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
    PostgreSQLContainer<?> runningContainer = (PostgreSQLContainer<?>) postgresql;

    return runningContainer.getJdbcUrl();
  }

  @NotNull
  private static String getDBUserName() {
    PostgreSQLContainer<?> runningContainer = (PostgreSQLContainer<?>) postgresql;

    return runningContainer.getUsername();
  }

  @NotNull
  private static String getDBPassword() {
    PostgreSQLContainer<?> runningContainer = (PostgreSQLContainer<?>) postgresql;

    return runningContainer.getPassword();
  }

  @AfterAll
  static void shutDown() {
    actorSystem.terminate();
  }
}