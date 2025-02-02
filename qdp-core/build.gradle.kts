plugins {
    `java-library`
    `maven-publish`
}

group = "com.quantori.qdp"

repositories {
    mavenLocal()
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
    withSourcesJar()
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

description = "QDP Core"
version = "1.0.0-SNAPSHOT"

dependencies {
    implementation("com.lightbend.akka.discovery:akka-discovery-aws-api_2.13:1.5.0-M1")
    implementation("com.typesafe.akka:akka-discovery_2.13:2.9.0-M2")
    implementation("com.lightbend.akka.management:akka-management_2.13:1.5.0-M1")
    implementation("com.lightbend.akka.management:akka-management-cluster-bootstrap_2.13:1.5.0-M1")

    implementation("com.lightbend.akka:akka-stream-alpakka-slick_2.13:6.0.2")
    implementation("com.typesafe.akka:akka-stream_2.13:2.9.0-M2")
    implementation("com.typesafe:config:1.4.2")

    implementation("com.typesafe.akka:akka-actor-typed_2.13:2.9.0-M2")
    implementation("com.typesafe.akka:akka-slf4j_2.13:2.9.0-M2")
    implementation("com.typesafe.akka:akka-bom_2.13:2.9.0-M2")
    implementation("com.typesafe.akka:akka-stream-typed_2.13:2.9.0-M2")
    implementation("com.typesafe.akka:akka-serialization-jackson_2.13:2.9.0-M2")
    implementation("com.typesafe.akka:akka-cluster-typed_2.13:2.9.0-M2")

    implementation(libs.common.text)
    implementation(libs.javax.validation)
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    testImplementation("com.typesafe.akka:akka-actor-testkit-typed_2.13:2.9.0-M2")
    testImplementation("com.typesafe.akka:akka-testkit_2.13:2.9.0-M2")
    testImplementation("org.mockito:mockito-core:4.11.0")
    testImplementation("org.mockito:mockito-inline:4.11.0")
    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("org.junit.jupiter:junit-jupiter-params")
    testImplementation("org.junit.jupiter:junit-jupiter-engine")
    testImplementation("org.assertj:assertj-core:3.21.0")
    testImplementation("org.awaitility:awaitility:4.1.0")
    testImplementation(libs.bundles.testcontainers)
    testImplementation(libs.lombok)
    testAnnotationProcessor(libs.lombok)
}
