package com.quantori.cqp.core.configuration;

import akka.actor.typed.ActorSystem;
import com.quantori.cqp.core.source.SourceRootActor;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

class ECSClusterProviderTest {

    @Test
    void mockedECSClusterStartsWithFallbackConfiguration() throws Exception {
        ECSClusterProvider ecsClusterProvider = new ECSClusterProvider();

        String metadataUri = "http://ecsContainerMetadata.Uri";
        ClusterConfigurationProperties properties =
            ClusterConfigurationProperties.builder()
                .clusterHostName("localhost")
                .clusterPort(8080)
                .maxSearchActors(100)
                .ecsContainerMetadataUri(metadataUri)
                .seedNodes(List.of("localhost:8081"))
                .build();

        HashMap<String, Object> map = new HashMap<>();
        ActorSystem<SourceRootActor.Command> system = null;
        try (MockedStatic<ECSConfigurationProvider> configProvider =
                 Mockito.mockStatic(ECSConfigurationProvider.class)) {
            configProvider
                .when(() -> ECSConfigurationProvider.getConfiguration(metadataUri))
                .thenReturn(map);

            system = ecsClusterProvider.actorTypedSystem(properties);

            Thread.sleep(2001);

            long uptime = system.uptime();

            assertThat(uptime, is(notNullValue()));
            assertThat(uptime, is(greaterThan(2L)));
        } finally {
            Objects.requireNonNull(system).terminate();
            Await.result(system.whenTerminated(), Duration.apply(5, TimeUnit.SECONDS));
        }
    }
}
