package com.quantori.cqp.build

import org.gradle.api.JavaVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.plugins.JavaLibraryPlugin
import org.gradle.external.javadoc.StandardJavadocDocletOptions
import org.gradle.jvm.toolchain.JavaLanguageVersion
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.*

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
                            developer {
                                id.set("artem.chukin")
                                name.set("Artem Chukin")
                                email.set("artem.chukin@quantori.com")
                            }
                            developer {
                                id.set("dmitriy.gusev")
                                name.set("Dmitriy Gusev")
                                email.set("dmitriy.gusev@quantori.com")
                            }
                            developer {
                                id.set("valeriy.burmistrov")
                                name.set("Valeriy Burmistrov")
                                email.set("valeriy.burmistrov@quantori.com")
                            }
                            developer {
                                id.set("boris.sukhodoev")
                                name.set("Boris Sukhodoev")
                                email.set("boris.sukhodoev@quantori.com")
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
            val signingSecretKey = project.findProperty("signing.secretKey") as String? ?: System.getenv("GPG_SIGNING_SECRET_KEY")
            val signingPassword = project.findProperty("signing.password") as String? ?: System.getenv("GPG_SIGNING_PASSWORD")
            useInMemoryPgpKeys(signingSecretKey, signingPassword)

            sign(project.configurations.getByName("archives"))
            sign(project.extensions.getByType(org.gradle.api.publish.PublishingExtension::class).publications["mavenJava"])
        }

        project.tasks.named("test", Test::class) {
            useJUnitPlatform()
            testLogging {
                events("passed", "skipped", "failed")
            }
        }
    }
}
