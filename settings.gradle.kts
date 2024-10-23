rootProject.name = "prj-bik-qdp"
include ("qdp-core")
include ("qdp-storage:api")
findProject(":qdp-storage:api")?.name = "api"
include ("qdp-storage:solr")
findProject(":qdp-storage:solr")?.name = "solr"
include ("qdp-storage:postgresql")
findProject(":qdp-storage:postgresql")?.name = "postgresql"
include ("qdp-storage:elasticsearch")
findProject(":qdp-storage:elasticsearch")?.name = "elasticsearch"
include ("qdp-storage:storage-tests")
findProject(":qdp-storage:storage-tests")?.name = "storage-tests"


