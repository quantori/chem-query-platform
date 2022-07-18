package com.quantori.qdp.core.task.dao;


import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

@Testcontainers
public class ContainerizedTest {

  @Container
  public static PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>("postgres:latest")
      .withCopyFileToContainer(MountableFile.forClasspathResource("initdb.sql"), "/")
      .withInitScript("initdb.sql");

}
