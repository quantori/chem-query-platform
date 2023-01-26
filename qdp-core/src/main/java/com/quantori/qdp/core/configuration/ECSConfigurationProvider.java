package com.quantori.qdp.core.configuration;

import com.amazonaws.services.ecs.AmazonECSAsync;
import com.amazonaws.services.ecs.AmazonECSAsyncClientBuilder;
import com.amazonaws.services.ecs.model.DescribeTasksRequest;
import com.amazonaws.services.ecs.model.DescribeTasksResult;
import com.amazonaws.services.ecs.model.Task;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

public class ECSConfigurationProvider {
  private static final int CONNECTION_TIMEOUT_SECONDS = 10;
  private static final String LABELS = "Labels";
  private static final String TASK_ARN_PROPERTY_NAME = "com.amazonaws.ecs.task-arn";
  private static final String CLUSTER_ARN_PROPERTY_NAME = "com.amazonaws.ecs.cluster";

  private ECSConfigurationProvider() {

  }

  public static Map<String, String> getConfiguration(String metadataUri) {
    try {
      JsonNode json = getTaskMetadata(metadataUri);
      String taskArn = getTaskArn(json);
      String clusterArn = getClusterArn(json);

      AmazonECSAsync client = AmazonECSAsyncClientBuilder.defaultClient();
      DescribeTasksResult result = client.describeTasks(
          new DescribeTasksRequest()
              .withCluster(clusterArn)
              .withTasks(taskArn));

      Task taskDetails = result.getTasks()
          .stream()
          .findFirst()
          .orElseThrow();

      return Map.of(
          "akka.remote.artery.canonical.hostname", getHostIp(taskDetails),
          "akka.management.http.hostname", getHostIp(taskDetails),
          "akka.management.cluster.bootstrap.contact-point-discovery.service-name", getServiceName(taskDetails),
          "akka.discovery.aws-api-ecs.cluster", getClusterName(taskDetails)
      );

    } catch (InterruptedException e) {
      Thread.currentThread()
          .interrupt();

      throw new ECSConfigurationException("The procedure to fetch task info was interrupted", e);
    } catch (URISyntaxException | IOException e) {
      throw new ECSConfigurationException("Error to fetch task info", e);
    }
  }

  private static String getTaskArn(JsonNode taskMetadata) {
    return taskMetadata.get(LABELS)
        .get(TASK_ARN_PROPERTY_NAME)
        .asText();
  }

  private static String getClusterArn(JsonNode taskMetadata) {
    return taskMetadata.get(LABELS)
        .get(CLUSTER_ARN_PROPERTY_NAME)
        .asText();
  }

  private static String getHostIp(Task taskDetails) {
    return taskDetails.getContainers()
        .get(0)
        .getNetworkInterfaces()
        .get(0)
        .getPrivateIpv4Address();
  }

  private static String getServiceName(Task taskDetails) {
    return taskDetails.getGroup()
        .split("service:")[1];
  }

  private static String getClusterName(Task taskDetails) {
    return taskDetails.getClusterArn()
        .split("cluster/")[1];
  }

  private static JsonNode getTaskMetadata(String metadataUri)
      throws URISyntaxException, IOException, InterruptedException {
    HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(CONNECTION_TIMEOUT_SECONDS))
        .build();

    HttpResponse<String> response = httpClient.send(HttpRequest.newBuilder(new URI(metadataUri))
            .header("Accept", "application/json")
            .build(),
        HttpResponse.BodyHandlers.ofString());

    return new ObjectMapper()
        .readTree(response.body());
  }

  private static class ECSConfigurationException extends RuntimeException {
    public ECSConfigurationException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
