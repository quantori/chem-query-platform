plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.5.0"
}
rootProject.name = "chem-query-platform"

include("cqp-core")
include("cqp-api")
include("cqp-storage-elasticsearch")
includeBuild("cqp-build")
