package com.quantori.qdp.core.task.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The class need to be used for tasks which need to be persisted and restarted in cases where the
 * task was interrupted or server was restarted by some incident
 */
public abstract class ResumableTaskDescription extends StreamTaskDescription {

  private final TaskDescriptionSerDe serDe;

  public ResumableTaskDescription(
      @JsonProperty("provider") DataProvider provider,
      @JsonProperty("function") StreamTaskFunction function,
      @JsonProperty("aggregator") ResultAggregator aggregator,
      @JsonProperty("factory") TaskDescriptionSerDe serDe,
      @JsonProperty("user") String user,
      @JsonProperty("type") String type) {
    super(provider, function, aggregator, user, type);
    this.serDe = serDe;
  }

  public TaskDescriptionSerDe getSerDe() {
    return serDe;
  }

  public abstract DescriptionState getState();
}
