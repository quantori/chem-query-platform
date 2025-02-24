package com.quantori.qdp.core.task;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.util.HashMap;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
public abstract class ContainerizedTest {

  @Container
  public static PostgreSQLContainer<?> postgresDBContainer =
      new PostgreSQLContainer<>("postgres:latest")
          .withCopyFileToContainer(MountableFile.forClasspathResource("initdb.sql"), "/")
          .withInitScript("initdb.sql");

  protected void reinitTable() throws IOException, InterruptedException {
    postgresDBContainer.execInContainer(
        "psql",
        "-U",
        postgresDBContainer.getUsername(),
        "-d",
        postgresDBContainer.getDatabaseName(),
        "-f",
        "/initdb.sql");
  }

  protected static Config getSlickConfig() {
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
  protected static String getDBUrlString() {
    return postgresDBContainer.getJdbcUrl();
  }

  @NotNull
  protected static String getDBUserName() {
    return postgresDBContainer.getUsername();
  }

  @NotNull
  protected static String getDBPassword() {
    return postgresDBContainer.getPassword();
  }
}
