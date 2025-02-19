package com.quantori.qdp.core.task.model;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

@SuppressWarnings("unused")
public class StreamTaskDescription {

  private final DataProvider provider;
  private final StreamTaskFunction function;
  private final ResultAggregator aggregator;
  private final String user;
  private final String type;
  private Consumer<StreamTaskResult> subscription = null;
  private float weight = 1f;
  private Supplier<Map<String, String>> detailsSupplier;

  public StreamTaskDescription(
      DataProvider provider,
      StreamTaskFunction function,
      ResultAggregator aggregator,
      String user,
      String type) {
    this.provider = provider;
    this.function = function;
    this.aggregator = aggregator;
    this.detailsSupplier = Map::of;
    this.user = user;
    this.type = type;
  }

  public DataProvider getProvider() {
    return provider;
  }

  public StreamTaskFunction getFunction() {
    return function;
  }

  public ResultAggregator getAggregator() {
    return aggregator;
  }

  /**
   * The subscription might be useful in a Flow where several tasks are executed. Using the
   * subscription the task might get a result from previously executed task. The consumer return
   * Null - if there was no previously executed task.
   *
   * @return a consumer for a previous task result in a flow
   */
  public Consumer<StreamTaskResult> getSubscription() {
    return subscription;
  }

  /**
   * The subscription might be useful in a Flow where several tasks are executed. Using the
   * subscription the task might get a result from previously executed task. The consumer return
   * Null - if there was no previously executed task.
   *
   * @param subscription - consumer for previous task results
   * @return StreamTaskDescription
   */
  public StreamTaskDescription setSubscription(Consumer<StreamTaskResult> subscription) {
    this.subscription = subscription;
    return this;
  }

  /**
   * The weight is used for percent calculation in a flow where several tasks are executed.
   *
   * @return weight of the task in a flow
   */
  public float getWeight() {
    return weight;
  }

  /**
   * The weight is used for percent calculation in a flow where several tasks are executed. Default
   * value 1.0
   *
   * @param weight - weight of the task in a flow
   * @return StreamTaskDescription
   */
  public StreamTaskDescription setWeight(float weight) {
    this.weight = weight;
    return this;
  }

  public String getUser() {
    return user;
  }

  /**
   * Method return a supplier for the task description in Map format
   *
   * @return supplier of Map
   */
  public Supplier<Map<String, String>> getDetailsMapProvider() {
    return detailsSupplier;
  }

  public StreamTaskDescription setDetailsSupplier(Supplier<Map<String, String>> detailsSupplier) {
    this.detailsSupplier = detailsSupplier;
    return this;
  }

  public String getType() {
    return type;
  }
}
