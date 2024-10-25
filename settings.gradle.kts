rootProject.name = "prj-bik-qdp"

include("qdp-core")
include("qdp-storage:api")
findProject(":qdp-storage:api")?.name = "qdp-storage-api"
include("qdp-storage:solr")
findProject(":qdp-storage:solr")?.name = "qdp-storage-solr"
include("qdp-storage:postgresql")
findProject(":qdp-storage:postgresql")?.name = "qdp-storage-postgresql"
include("qdp-storage:elasticsearch")
findProject(":qdp-storage:elasticsearch")?.name = "qdp-storage-elasticsearch"
include("qdp-storage:storage-tests")
findProject(":qdp-storage:storage-tests")?.name = "storage-tests"


