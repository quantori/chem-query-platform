import java.net.HttpURLConnection
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

plugins {
    `java-library`
    `maven-publish`
    signing
    id("com.diffplug.spotless") version "7.0.2"
}

group = "com.quantori"
description = "Chem query platform. Compound quick search"
version = "0.0.10"

repositories {
    mavenLocal()
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
    withSourcesJar()
    withJavadocJar()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])

            pom {
                artifactId = "cqp-core"
                name = project.name
                description = project.description
                packaging = "jar"
                url = "https://github.com/quantori/chem-query-platform"

                licenses {
                    license {
                        name = "The Apache License, Version 2.0"
                        url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                    }
                }

                developers {
                    developer {
                        id = "artem.chukin"
                        name = "Artem Chukin"
                        email = "artem.chukin@quantori.com"
                    }
                    developer {
                        id = "dmitriy gusev"
                        name = "Dmitriy Gusev"
                        email = "dmitriy.gusev@quantori.com"
                    }

                    developer {
                        id = "valeriy burmistrov"
                        name = "Valeriy Burmistrov"
                        email = "valeriy.burmistrov@quantori.com"
                    }

                    developer {
                        id = "boris sukhodoev"
                        name = "Boris Sukhodoev"
                        email = "boris.sukhodoev@quantori.com"
                    }
                }

                scm {
                    connection = "scm:git:git://github.com/quantori/chem-query-platform.git"
                    developerConnection = "scm:git:ssh://github.com/quantori/chem-query-platform.git"
                    url = "https://github.com/quantori/chem-query-platform"
                }
            }
        }
    }
    repositories {
        maven {
            name = "localStaging"
            // change URLs to point to your repos, e.g. http://my.org/repo
            val releasesRepoUrl = uri(layout.buildDirectory.dir("repos/releases"))
            val snapshotsRepoUrl = uri(layout.buildDirectory.dir("repos/snapshots"))
            url = if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl
        }
    }
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

artifacts {
    archives(tasks.named("javadocJar"))
    archives(tasks.named("sourcesJar"))
}

signing {
    setRequired {
        !version.toString().endsWith("SNAPSHOT") && gradle.taskGraph.hasTask("publish")
    }
    val signingSecretKey = findProperty("signing.secretKey") as String? ?: System.getenv("GPG_SIGNING_SECRET_KEY")
    val signingPassword = findProperty("signing.password") as String? ?: System.getenv("GPG_SIGNING_PASSWORD")

    useInMemoryPgpKeys(signingSecretKey, signingPassword)
    sign(publishing.publications["mavenJava"])
    sign(configurations.archives.get())
}

// Fix Javadoc warnings on JDK 9+ (optional but recommended)
if (JavaVersion.current().isJava9Compatible) {
    tasks.withType<Javadoc>().configureEach {
        (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
        (options as StandardJavadocDocletOptions).addBooleanOption("html5", true)
    }
}

@OptIn(ExperimentalEncodingApi::class)
fun String.toBase64(): String {
    return Base64.encode(this.toByteArray(Charsets.UTF_8))
}

val publishToLocalStaging = tasks.getByName("publishMavenJavaPublicationToLocalStagingRepository")

publishToLocalStaging.outputs.dir(layout.buildDirectory.dir("repos/releases"))

val zipBundle by tasks.registering(Zip::class) {
    archiveFileName = "central-bundle.zip"
    destinationDirectory = project.layout.buildDirectory.dir("distributions")
    inputs.files(publishToLocalStaging.outputs.files)
    from(publishToLocalStaging.outputs.files.files)
}

val uploadToMavenCentral by tasks.registering {
    val url = uri("https://central.sonatype.com/api/v1/publisher/upload").toURL()
    val boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW"
    inputs.file(zipBundle.map { it.archiveFile }.get())
    doLast {
        val file = zipBundle.get().archiveFile.get().asFile
        val mavenCentralUsername =
                findProperty("mavenCentralUsername") as String? ?: System.getenv("MAVEN_CENTRAL_USERNAME")
        val mavenCentralPassword =
                findProperty("mavenCentralPassword") as String? ?: System.getenv("MAVEN_CENTRAL_PASSWORD")
        val token = "$mavenCentralUsername:$mavenCentralPassword\n".toBase64()

        val connection = (url.openConnection() as HttpURLConnection).apply {
            requestMethod = "POST"
            doOutput = true
            setRequestProperty("Authorization", "Bearer $token")
            setRequestProperty("Content-Type", "multipart/form-data; boundary=$boundary")
        }

        val outputStream = connection.outputStream
        outputStream.bufferedWriter().use { writer ->
            writer.append("--$boundary\r\n")
            writer.append("Content-Disposition: form-data; name=\"bundle\"; filename=\"${file.name}\"\r\n")
            writer.append("Content-Type: application/octet-stream\r\n\r\n")
            writer.flush()

            file.inputStream().use { it.copyTo(outputStream) }

            writer.append("\r\n--$boundary--\r\n")
            writer.flush()
        }

        val responseCode = connection.responseCode
        println("Response Code: $responseCode")
        println("Response Message: ${connection.inputStream.bufferedReader().readText()}")
    }
}

dependencies {
    implementation("commons-codec:commons-codec:1.15")
    compileOnly(libs.jackson)

    implementation(libs.javax.validation)
    implementation(libs.bundles.indigo)
    implementation(libs.common.text)

    compileOnly(libs.lombok)
    annotationProcessor(libs.lombok)

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