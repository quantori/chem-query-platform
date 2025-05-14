package com.quantori.cqp.build

import org.gradle.api.JavaVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.plugins.JavaLibraryPlugin
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.external.javadoc.StandardJavadocDocletOptions
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.api.tasks.testing.Test
import org.gradle.api.tasks.bundling.Zip
import org.gradle.kotlin.dsl.*

import java.net.HttpURLConnection
import java.nio.charset.StandardCharsets
import java.util.Base64

class CqpJavaLibraryPlugin : Plugin<Project> {
    override fun apply(project: Project) {
        project.pluginManager.apply(JavaLibraryPlugin::class)
        project.pluginManager.apply("maven-publish")
        project.pluginManager.apply("signing")

        project.group = "com.quantori"
        project.version = project.findProperty("version") ?: "0.0.1-SNAPSHOT"

        project.repositories {
            mavenLocal()
            mavenCentral()
        }

        project.extensions.configure<JavaPluginExtension> {
            toolchain.languageVersion.set(JavaLanguageVersion.of(17))
            withSourcesJar()
            withJavadocJar()
        }

        project.tasks.withType<org.gradle.api.tasks.javadoc.Javadoc>().configureEach {
            if (JavaVersion.current().isJava9Compatible) {
                (options as StandardJavadocDocletOptions).apply {
                    addStringOption("Xdoclint:none", "-quiet")
                    addBooleanOption("html5", true)
                }
            }
        }

        project.extensions.configure<org.gradle.api.publish.PublishingExtension> {
            publications {
                create<MavenPublication>("mavenJava") {
                    from(project.components["java"])

                    pom {
                        artifactId = project.name
                        name.set(project.name)
                        description.set(project.description ?: "")
                        url.set("https://github.com/quantori/chem-query-platform")

                        licenses {
                            license {
                                name.set("The Apache License, Version 2.0")
                                url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                            }
                        }

                        developers {
                            listOf(
                                    Triple("artem.chukin", "Artem Chukin", "artem.chukin@quantori.com"),
                                    Triple("dmitriy.gusev", "Dmitriy Gusev", "dmitriy.gusev@quantori.com"),
                                    Triple("valeriy.burmistrov", "Valeriy Burmistrov", "valeriy.burmistrov@quantori.com"),
                                    Triple("boris.sukhodoev", "Boris Sukhodoev", "boris.sukhodoev@quantori.com")
                            ).forEach { (id, name, email) ->
                                developer {
                                    this.id.set(id)
                                    this.name.set(name)
                                    this.email.set(email)
                                }
                            }
                        }

                        scm {
                            connection.set("scm:git:git://github.com/quantori/chem-query-platform.git")
                            developerConnection.set("scm:git:ssh://github.com/quantori/chem-query-platform.git")
                            url.set("https://github.com/quantori/chem-query-platform")
                        }
                    }
                }
            }

            repositories {
                maven {
                    name = "localStaging"
                    val releasesRepoUrl = project.layout.buildDirectory.dir("repos/releases")
                    val snapshotsRepoUrl = project.layout.buildDirectory.dir("repos/snapshots")
                    url = project.uri(
                            if (project.version.toString().endsWith("SNAPSHOT"))
                                snapshotsRepoUrl
                            else
                                releasesRepoUrl
                    )
                }
            }
        }

        project.extensions.configure<org.gradle.plugins.signing.SigningExtension> {
            setRequired {
                !project.version.toString().endsWith("SNAPSHOT") &&
                        project.gradle.taskGraph.hasTask("publish")
            }

            val signingSecretKey = project.findProperty("signing.secretKey") as String?
                    ?: System.getenv("GPG_SIGNING_SECRET_KEY")
            val signingPassword = project.findProperty("signing.password") as String?
                    ?: System.getenv("GPG_SIGNING_PASSWORD")

            if (!signingSecretKey.isNullOrBlank() && !signingPassword.isNullOrBlank()) {
                useInMemoryPgpKeys(signingSecretKey, signingPassword)

                sign(project.configurations.getByName("archives"))
                sign(project.extensions.getByType(org.gradle.api.publish.PublishingExtension::class).publications["mavenJava"])
            } else {
                project.logger.lifecycle("GPG signing skipped: missing credentials")
            }
        }

        project.tasks.named("test", Test::class) {
            useJUnitPlatform()
            testLogging {
                events("passed", "skipped", "failed")
            }
        }

        fun String.toBase64(): String {
            return Base64.getEncoder().encodeToString(this.toByteArray(StandardCharsets.UTF_8))
        }

        project.afterEvaluate {
            val publishTask = project.tasks.findByName("publishMavenJavaPublicationToLocalStagingRepository")

            val zipBundle = project.tasks.register<Zip>("zipBundle") {
                group = "publishing"
                description = "Zips the published files for Maven Central upload"
                archiveFileName.set("central-bundle-${name}-${version}.zip")
                destinationDirectory.set(project.layout.buildDirectory.dir("distributions").get().asFile)
                dependsOn(publishTask)
                from(project.layout.buildDirectory.dir("repos/releases").get().asFile)
            }

            project.tasks.register<Task>("uploadToMavenCentral") {
                group = "publishing"
                description = "Uploads the central-bundle.zip to Maven Central via API"
                dependsOn(zipBundle)

                doLast {
                    val file = zipBundle.get().archiveFile.get().asFile
                    val boundary = "----WebKitFormBoundary7MA4YWxkTrZu0gW"
                    val url = uri("https://central.sonatype.com/api/v1/publisher/upload").toURL()

                    val username = project.findProperty("mavenCentralUsername") as String?
                            ?: System.getenv("MAVEN_CENTRAL_USERNAME")
                    val password = project.findProperty("mavenCentralPassword") as String?
                            ?: System.getenv("MAVEN_CENTRAL_PASSWORD")
                    val token = "$username:$password".toBase64()

                    val connection = url.openConnection() as HttpURLConnection
                    connection.requestMethod = "POST"
                    connection.doOutput = true
                    connection.setRequestProperty("Authorization", "Bearer $token")
                    connection.setRequestProperty("Content-Type", "multipart/form-data; boundary=$boundary")

                    connection.outputStream.use { output ->
                        output.writer().use { writer ->
                            writer.append("--$boundary\r\n")
                            writer.append("Content-Disposition: form-data; name=\"bundle\"; filename=\"${file.name}\"\r\n")
                            writer.append("Content-Type: application/octet-stream\r\n\r\n")
                            writer.flush()
                            file.inputStream().copyTo(output)
                            writer.append("\r\n--$boundary--\r\n")
                            writer.flush()
                        }
                    }

                    val responseCode = connection.responseCode
                    println("Response Code: $responseCode")
                    println("Response Message: ${connection.inputStream.bufferedReader().readText()}")
                }
            }
        }
    }
}
