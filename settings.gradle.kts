plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.5.0"
}
rootProject.name = "chem-query-platform"

include("cqp-core")
includeBuild("cqp-build")
