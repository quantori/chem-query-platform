package com.quantori.cqp.core.task.model;

/**
 * The interface needs to be implemented when a StreamTaskDescription needs to be serialized and
 * restored. This functionality is used for a task restart.
 */
public interface TaskDescriptionSerDe {

  /**
   * The methods restores StreamTaskDescription instance for a task restart
   *
   * @param json - json with data required for deserialization
   * @return StreamTaskDescription instance
   */
  StreamTaskDescription deserialize(String json);

  /**
   * The method serializes all required data about this task in json format.
   *
   * @return json as a string
   */
  String serialize(DescriptionState state);

  void setRequiredEntities(Object entityHolder);
}
