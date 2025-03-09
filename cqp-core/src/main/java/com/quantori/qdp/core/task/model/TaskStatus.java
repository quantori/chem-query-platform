package com.quantori.qdp.core.task.model;

import java.util.Date;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Accessors(chain = true)
public class TaskStatus {
  private UUID taskId;
  private StreamTaskStatus.Status status;
  private String type;
  private int restartFlag;
  private String flowId;
  private String deserializer;
  private String user;
  private Date createdDate;
  private Date updatedDate;
  private String state;
  private int parallelism;
  private int buffer;
}
