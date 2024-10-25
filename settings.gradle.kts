rootProject.name = "prj-bik-qdp"

dependencyResolutionManagement {
    versionCatalogs {
        create("libs") {
            version("lombok", "1.18.28")
            version("indigo", "1.22.0-rc.2")
            version("testcontainers", "1.17.6")

            library("common-text", "org.apache.commons", "commons-text").version("1.10.0")

            library("lombok", "org.projectlombok", "lombok").versionRef("lombok")

            library("indigo", "com.epam.indigo", "indigo").versionRef("indigo")
            library("indigo-inchi", "com.epam.indigo", "indigo-inchi").versionRef("indigo")
            library("indigo-renderer", "com.epam.indigo", "indigo-renderer").versionRef("indigo")

            library("testcontainers", "org.testcontainers", "testcontainers").versionRef("testcontainers")
            library("testcontainers-elasticsearch", "org.testcontainers", "elasticsearch").versionRef("testcontainers")
            library("testcontainers-solr", "org.testcontainers", "solr").versionRef("testcontainers")
            library("testcontainers-postgresql", "org.testcontainers", "postgresql").versionRef("testcontainers")
            library("testcontainers-junit", "org.testcontainers", "junit-jupiter").versionRef("testcontainers")

            bundle("indigo", listOf("indigo", "indigo-inchi", "indigo-renderer"))
            bundle(
                "testcontainers",
                listOf(
                    "testcontainers",
                    "testcontainers-elasticsearch",
                    "testcontainers-solr",
                    "testcontainers-postgresql",
                    "testcontainers-junit"
                )
            );
        }
    }
}


include("qdp-core")
include("qdp-storage:api")
findProject(":qdp-storage:api")?.name = "api"
include("qdp-storage:solr")
findProject(":qdp-storage:solr")?.name = "solr"
include("qdp-storage:postgresql")
findProject(":qdp-storage:postgresql")?.name = "postgresql"
include("qdp-storage:elasticsearch")
findProject(":qdp-storage:elasticsearch")?.name = "elasticsearch"
include("qdp-storage:storage-tests")
findProject(":qdp-storage:storage-tests")?.name = "storage-tests"


