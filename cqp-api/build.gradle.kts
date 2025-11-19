plugins {
    `java-library`
    id("com.diffplug.spotless") version "7.0.2"
    id("com.quantori.cqp-build")
}

group = "com.quantori"
description = "Chem query platform. Storage API"
version = "0.0.17"

dependencies {
    implementation("commons-codec:commons-codec:1.15")

    implementation(libs.javax.validation)
    implementation(libs.bundles.indigo)
    implementation(libs.common.text)
    compileOnly(libs.jackson)

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter-api")
    testImplementation("org.junit.jupiter:junit-jupiter-params")
    testImplementation("org.junit.jupiter:junit-jupiter-engine")

    testImplementation("org.mockito:mockito-core:4.11.0")
    testImplementation("org.mockito:mockito-inline:4.11.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.12.0")
    testImplementation("org.assertj:assertj-core:3.21.0")
    testImplementation("org.awaitility:awaitility:4.1.0")
    testImplementation(libs.bundles.testcontainers)

    testImplementation(libs.jackson)
    testImplementation(libs.lombok)
    testAnnotationProcessor(libs.lombok)
}
