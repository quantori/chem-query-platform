# prj-bik-qdp CI/CD

## Jenkins configuration
### Prerequisites
1. Jenkins plugins:
    - Nexus Artifact Uploader

### Configuration
1. Jenkins menu
2. create new job
3. pipeline
4. select this project is parameterized
5. add string parameters
    - GIT_URL : git@github.com:quantori/prj-bik-qdp.git
    - GIT_BRANCH : <git branch>
    - GIT_CREDENTIALS_ID : github (from Jenkins credentials)
    - NEXUS_URL : repo.qtidev.com:8081
    - NEXUS_CREDENTIALS_ID : nexu (from Jenkins credentials)
6. add choice parameter
    - snapshot and release mode choice
7. pipeline script from SCM-git
8. Config Repository and Branch and Script Patch :
    - Repository URL : ${GIT_URL}
    - Select credentials : ${GIT_CREDENTIALS_ID}
    - Branch Specifier : <git branch>
    - Script Path : qdp-core/etc/jenkins/Jenkinsfile