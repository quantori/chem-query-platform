val lombokVersion: String by project
val testcontainersVersion: String by project

description = "QDP Core"

dependencies {
    implementation(project(":qdp-storage:api"))

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
    implementation("org.apache.commons:commons-text:1.10.0")
    implementation("com.typesafe.akka:akka-cluster-typed_2.13:2.9.0-M2")
    implementation("javax.validation:validation-api:2.0.1.Final")
    compileOnly("org.projectlombok:lombok:${lombokVersion}")
    annotationProcessor("org.projectlombok:lombok:${lombokVersion}")

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
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:postgresql:$testcontainersVersion")
    testImplementation("org.testcontainers:solr:$testcontainersVersion")
    testImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    testImplementation("org.projectlombok:lombok:${lombokVersion}")
    testAnnotationProcessor("org.projectlombok:lombok:${lombokVersion}")
}
