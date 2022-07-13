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
//@EntityListeners(AuditingEntityListener.class)
public class TaskStatus {

//    @Id
//    @Column(name = "id")
    UUID taskId;

//    @Column(name = "status")
    StreamTaskStatus.Status status;

//    @Column(name = "task_type")
    StreamTaskDetails.TaskType type;

//    @Column(name = "restart_flag")
    int restartFlag;

//    @Column(name = "flow_id")
    String flowId;

//    @Column(name = "deserializer")
    String deserializer;

//    @Column(name = "created_by")
    String user;

//    @Column(nullable = false, updatable = false)
//    @CreatedDate
    Date createdDate;

//    @Column(nullable = false)
//    @LastModifiedDate
    Date updatedDate;

//    @Column(name = "state")
    String state;

//    @Column(name = "parallelism")
    int parallelism;

//    @Column(name = "buffer")
    int buffer;
}
