//package com.quantori.qdp.core.task;
//
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.awaitility.Awaitility.await;
//import static org.junit.jupiter.api.Assertions.assertThrows;
//
//import com.quantori.qdp.core.TestUtils;
//import com.quantori.qdp.core.auth.model.AuthTokenService;
//import com.quantori.qdp.core.cognito.auth.AuthService;
//import com.quantori.qdp.core.config.RealTestConfig;
//import com.quantori.qdp.core.data.controller.dto.MergeDescriptionRequest;
//import com.quantori.qdp.core.data.service.FileParsingService;
//import com.quantori.qdp.core.data.service.MergeIndexesService;
//import com.quantori.qdp.core.filequery.controller.FileQueryResource;
//import com.quantori.qdp.core.subscription.model.SubscriptionService;
//import com.quantori.qdp.core.task.model.RunningMergeTaskException;
//import com.quantori.qdp.core.task.model.StreamTaskStatus;
//import com.quantori.qdp.core.task.service.MergeTaskChecker;
//import com.quantori.qdp.core.task.service.StreamTaskService;
//import com.quantori.qdp.core.task.ContainerizedTest;
//import java.io.IOException;
//import java.time.Duration;
//import java.util.List;
//import java.util.concurrent.ExecutionException;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.boot.test.web.client.TestRestTemplate;
//import org.springframework.http.HttpHeaders;
//import org.springframework.http.MediaType;
//import org.springframework.test.context.ActiveProfiles;
//
//@ActiveProfiles("test")
//@SpringBootTest(classes = RealTestConfig.class,
//        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
//public class MergeTaskCheckerTest extends ContainerizedTest {
//
//    private final HttpHeaders headers = new HttpHeaders();
//
//    @Autowired
//    MergeTaskChecker mergeTaskChecker;
//    @Autowired
//    private MergeIndexesService mergeIndexesService;
//    @Autowired
//    private TestRestTemplate restTemplate;
//    @Autowired
//    private AuthService authService;
//    @Autowired
//    private AuthTokenService authTokenService;
//    @Autowired
//    private SubscriptionService subscriptionService;
//    @Autowired
//    private FileParsingService fileParsingService;
//    @Autowired
//    private StreamTaskService streamTaskService;
//
//    private String indexId1;
//    private String indexId2;
//    private String user;
//
//
//    @BeforeEach
//    void before() throws Exception {
//        user = TestUtils.testAuth(authTokenService, authService, headers);
//        TestUtils.setTestData(subscriptionService, authService);
//        indexId1 = uploadFile("17str.sdf", 17);
//        indexId2 = uploadFile("30str.sdf", 30);
//        headers.setContentType(MediaType.APPLICATION_JSON);
//    }
//
//    private String uploadFile(String name, int size) throws IOException {
//        var request = TestUtils.getUploadFileRequestEntity(name, headers);
//        FileQueryResource fileUploadResource = restTemplate.postForObject("/upload", request, FileQueryResource.class);
//        assertThat(fileUploadResource).isNotNull();
//        assertThat(fileUploadResource.id).isNotNull();
//        await().atMost(Duration.ofSeconds(5)).until(() -> TestUtils.checkFileParsing(fileUploadResource.id, fileParsingService, size));
//        return fileUploadResource.id;
//    }
//
//    @Test
//    void checkOngoingMergeTasksTest() throws ExecutionException, InterruptedException {
//        var mergeRequest = new MergeDescriptionRequest(List.of(indexId1, indexId2), true, "someFile");
//        var taskId = mergeIndexesService.startMerge(mergeRequest, user).toCompletableFuture().get().taskId();
//        assertThrows(RunningMergeTaskException.class, () -> mergeTaskChecker.checkOngoingMergeTasks(user, indexId1));
//        await().atMost(Duration.ofSeconds(5)).until(() -> {
//            var status = streamTaskService.getTaskStatus(taskId, user).toCompletableFuture().get();
//            return status.status()
//                    .equals(StreamTaskStatus.Status.COMPLETED);
//        });
//    }
//}
