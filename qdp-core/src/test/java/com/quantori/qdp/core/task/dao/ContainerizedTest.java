package com.quantori.qdp.core.task.dao;


import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ContainerizedTest {

  @Container
  public static GenericContainer<?> postgresql = new PostgreSQLContainer<>("postgres:latest")
//      .withUsername("postgres")
//      .withDatabaseName("tasks_test")
      .withInitScript("initdb.sql");

}
