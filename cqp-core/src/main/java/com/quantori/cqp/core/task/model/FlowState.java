package com.quantori.cqp.core.task.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class FlowState {
  private final int currentTaskNumber;
  private final List<String> tasksStates;
  private final List<String> taskFactories;
  private final String finalizerFactory;
  private final String finalizerData;
  private final List<Float> taskWeights;
  private final String lastTaskResult;
  private final String lastTaskResultType;
  private final String currentTaskId;

  @JsonCreator
  public FlowState(
      @JsonProperty("currentTaskNumber") int currentTaskNumber,
      @JsonProperty("tasksStates") List<String> tasksStates,
      @JsonProperty("taskFactories") List<String> taskFactories,
      @JsonProperty("finalizerFactory") String finalizerFactory,
      @JsonProperty("finalizerData") String finalizerData,
      @JsonProperty("taskWeights") List<Float> taskWeights,
      @JsonProperty("lastTaskResult") String lastTaskResult,
      @JsonProperty("lastTaskResultType") String lastTaskResultType,
      @JsonProperty("currentTaskId") String currentTaskId) {
    this.currentTaskNumber = currentTaskNumber;
    this.tasksStates = tasksStates;
    this.taskFactories = taskFactories;
    this.finalizerFactory = finalizerFactory;
    this.finalizerData = finalizerData;
    this.taskWeights = taskWeights;
    this.lastTaskResult = lastTaskResult;
    this.lastTaskResultType = lastTaskResultType;
    this.currentTaskId = currentTaskId;
  }

  public int getCurrentTaskNumber() {
    return currentTaskNumber;
  }

  public List<String> getTasksStates() {
    return tasksStates;
  }

  public String getFinalizerFactory() {
    return finalizerFactory;
  }

  public String getFinalizerData() {
    return finalizerData;
  }

  public List<Float> getTaskWeights() {
    return taskWeights;
  }

  public List<String> getTaskFactories() {
    return taskFactories;
  }

  public String getLastTaskResult() {
    return lastTaskResult;
  }

  public String getLastTaskResultType() {
    return lastTaskResultType;
  }

  public String getCurrentTaskId() {
    return currentTaskId;
  }
}
