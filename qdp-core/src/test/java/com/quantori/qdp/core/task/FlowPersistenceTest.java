package com.quantori.qdp.core.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;

import akka.actor.testkit.typed.javadsl.ActorTestKit;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.stream.alpakka.slick.javadsl.SlickSession;
import akka.stream.alpakka.slick.javadsl.SlickSession$;
import com.quantori.qdp.core.search.TaskServiceActor;
import com.quantori.qdp.core.source.MoleculeSourceRootActor;
import com.quantori.qdp.core.task.dao.TaskStatusDao;
import com.quantori.qdp.core.task.model.DataProvider;
import com.quantori.qdp.core.task.model.DescriptionState;
import com.quantori.qdp.core.task.model.FlowFinalizer;
import com.quantori.qdp.core.task.model.FlowFinalizerSerDe;
import com.quantori.qdp.core.task.model.ResultAggregator;
import com.quantori.qdp.core.task.model.ResumableTaskDescription;
import com.quantori.qdp.core.task.model.StreamTaskDescription;
import com.quantori.qdp.core.task.model.StreamTaskDetails;
import com.quantori.qdp.core.task.model.StreamTaskResult;
import com.quantori.qdp.core.task.model.StreamTaskStatus;
import com.quantori.qdp.core.task.model.TaskDescriptionSerDe;
import com.quantori.qdp.core.task.service.StreamTaskService;
import com.quantori.qdp.core.task.service.StreamTaskServiceImpl;
import com.quantori.qdp.core.task.service.TaskPersistenceService;
import com.quantori.qdp.core.task.service.TaskPersistenceServiceImpl;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.context.ApplicationContext;
//import org.springframework.test.context.ActiveProfiles;

@SuppressWarnings("unused")
//@ActiveProfiles("test")
//@SpringBootTest(classes = MockTestConfig.class)
class FlowPersistenceTest extends ContainerizedTest {

    private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
    private static ActorSystem<MoleculeSourceRootActor.Command> system;
    private static ActorTestKit actorTestKit;
    private static StreamTaskService service;
    private static TaskPersistenceService persistenceService;
    private static TaskStatusDao taskStatusDao;

    @BeforeAll
    static void setup() {
        system =
            ActorSystem.create(MoleculeSourceRootActor.create(100), "test-actor-system");
        actorTestKit = ActorTestKit.create(system);
        SlickSession session = SlickSession$.MODULE$.forConfig(getSlickConfig());
        system.classicSystem().registerOnTermination(session::close);
        taskStatusDao = new TaskStatusDao(session, system);

        Behavior<TaskServiceActor.Command> commandBehavior = TaskServiceActor.create();

        ActorRef<TaskServiceActor.Command> commandActorRef = actorTestKit.spawn(commandBehavior);
        service = new StreamTaskServiceImpl(system, commandActorRef, () -> persistenceService);
        persistenceService =
            new TaskPersistenceServiceImpl(system, commandActorRef, () -> service, taskStatusDao, new Object(), false);
    }

    @AfterEach
    void clearDb() throws IOException, InterruptedException {
        reinitTable();
    }

    @AfterAll
    static void tearDown() {
        actorTestKit.shutdownTestKit();
    }

    @Test
    void flowPersistence() throws ExecutionException, InterruptedException {
        Assertions.assertTrue(taskStatusDao.findAll().isEmpty());
        //start flow
        var status = service.processTaskFlowAsTask(getDescriptionList(), StreamTaskDetails.TaskType.Upload,
                new TestFinilizer(), "user").toCompletableFuture().get();
        assertThat(status.status()).isEqualTo(StreamTaskStatus.Status.IN_PROGRESS);

        //get in progress status
        var statusCheck = service.getTaskStatus(status.taskId(), "user").toCompletableFuture().get();
        assertEquals(StreamTaskStatus.Status.IN_PROGRESS, statusCheck.status());

        //wait second subtask started
        await().atMost(Duration.ofSeconds(5)).until(() ->
                taskStatusDao.findAll().size() == 3);
        await().atMost(Duration.ofSeconds(5)).until(() ->
                Aggregator.result.toString().contains("two"));

        //close flow
        service.closeTask(status.taskId(), "user");

        //get not exist actor
        await().atMost(Duration.ofSeconds(5)).until(() ->
                persistenceService.taskActorDoesNotExists(UUID.fromString(status.taskId())));

        //check persistence
        var task = taskStatusDao.findById(UUID.fromString(status.taskId()));
        Assertions.assertTrue(task.isPresent());
        assertThat(task.get().getStatus()).isEqualTo(StreamTaskStatus.Status.IN_PROGRESS);
        assertThat(task.get().getRestartFlag()).isZero();

        //resume flow execution
        persistenceService.resumeFlow(UUID.fromString(status.taskId()));

        //get completed status
        await().atMost(Duration.ofSeconds(5)).until(() ->
                service.getTaskStatus(status.taskId(), "user").toCompletableFuture().get().status().equals(StreamTaskStatus.Status.COMPLETED));

        //get flow result
        var result = service.getTaskResult(status.taskId(), "user").toCompletableFuture().get().toString();
        assertThat(result).contains("one").contains("two").contains("final processing");

        //check all statuses COMPLETED
        await().atMost(Duration.ofSeconds(5)).until(() ->
                taskStatusDao.findAll().stream().filter(e ->
                        e.getStatus().equals(StreamTaskStatus.Status.COMPLETED)).count() == 4);

        //get not exist actor
        service.closeTask(status.taskId(), "user");
        await().atMost(Duration.ofSeconds(5)).until(() ->
                persistenceService.taskActorDoesNotExists(UUID.fromString(status.taskId())));

        //cleanup
//        Thread.sleep(Duration.ofSeconds(90).toMillis());
//        persistenceService.restartInProgressTasks();
//        Assertions.assertTrue(taskStatusDao.findAll().isEmpty());
    }

    private List<StreamTaskDescription> getDescriptionList() {
        return List.of(getFastDescription(" one "),
                getDescription(" two "),
                getFastDescription(" three "));
    }

    private ResumableTaskDescription getDescription(String data) {
        return new ResumableTaskDescription(
                getDataProvider(),
                item -> new DataProvider.Data() {
                },
                new Aggregator(data),
                new SerDe(),
                "user",
                StreamTaskDetails.TaskType.BulkEdit
        ) {
            @Override
            public DescriptionState getState() {
                return null;
            }
        };
    }


    ResumableTaskDescription getFastDescription(String data) {
        return new ResumableTaskDescription(
                () -> List.<DataProvider.Data>of(new DataProvider.Data() {
                }).iterator(),
                item -> new DataProvider.Data() {
                },
                new Aggregator(data),
                new SerDe(),
                "user",
                StreamTaskDetails.TaskType.BulkEdit
        ) {
            @Override
            public DescriptionState getState() {
                return null;
            }
        };
    }


    private DataProvider getDataProvider() {
        return new DataProvider() {
            volatile boolean stop = false;

            @Override
            public Iterator<Data> dataIterator() {
                return new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return !stop;
                    }

                    @Override
                    public Data next() {
                        return new Data() {
                        };
                    }
                };
            }

            @Override
            public void close() {
                stop = true;
            }
        };
    }


    public static class Aggregator implements ResultAggregator {
        static StringBuffer result = new StringBuffer();
        String data;

        public Aggregator(String data) {
            this.data = data;
        }

        public void consume(DataProvider.Data item) {
            result.append(data).append("\n");
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                logger.error("error: ", e);
            }
        }

        @Override
        public StreamTaskResult getResult() {
            return new Result();
        }

        @Override
        public float getPercent() {
            return 0;
        }
    }

    public static class Result implements StreamTaskResult {
        @Override
        public String toString() {
            return Aggregator.result.toString();
        }
    }

    public static class SerDe implements TaskDescriptionSerDe {

        @Override
        public StreamTaskDescription deserialize(String json) {
            return new ResumableTaskDescription(
                    () -> List.<DataProvider.Data>of(new DataProvider.Data() {
                    }).iterator(),
                    data -> new DataProvider.Data() {
                    },
                    new Aggregator(json),
                    new SerDe(),
                    "user",
                    StreamTaskDetails.TaskType.BulkEdit
            ) {
                @Override
                public DescriptionState getState() {
                    return null;
                }
            };
        }

        @Override
        public String serialize(DescriptionState state) {
            return "final processing";
        }

        @Override
        public void setRequiredEntities(Object entityHolder) {

        }
    }

    public static class TestFinilizer implements FlowFinalizer {

        @Override
        public FlowFinalizerSerDe getSerializer() {
            return new TestFlowFinalizerFactory();
        }

        @Override
        public void accept(Boolean aBoolean) {

        }
    }

    public static class TestFlowFinalizerFactory implements FlowFinalizerSerDe {
        @Override
        public String serialize(Map<String, String> params) {
            return "serialized";
        }

        @Override
        public FlowFinalizer deserialize(String data) {
            return new TestFinilizer();
        }
    }
}