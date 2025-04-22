plugins {
    `java-library`
    id("com.diffplug.spotless") version "7.0.2"
    id("com.quantori.cqp-build")
}

group = "com.quantori"
description = "Chem query platform. Storage Elasticsearch"
version = "0.0.12"

tasks.named<Javadoc>("javadoc") {
    exclude(
            "**/ElasticsearchIterator.java",
            "**/MoleculeDocument.java",
    )
}

dependencies {
    implementation("com.quantori:cqp-api:0.0.14")
    implementation("com.quantori:cqp-core:0.0.13")
    implementation("co.elastic.clients:elasticsearch-java:8.6.2")
    implementation(libs.jackson)
    implementation(libs.jackson.jsr310)
    implementation(libs.javax.validation)

    implementation("org.slf4j:slf4j-api:1.7.36")
    implementation("org.slf4j:jcl-over-slf4j:1.7.36")
    implementation(libs.common.lang)

    compileOnly("io.swagger:swagger-models:1.5.20")
    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.0")
    testImplementation("org.mockito:mockito-core:5.12.0")
    testImplementation("org.mockito:mockito-junit-jupiter:5.12.0")
}

spotless {
    java {
        target("src/**/*.java")
//        googleJavaFormat()
        removeUnusedImports()
        trimTrailingWhitespace()
        endWithNewline()
    }
}
