## Build instructions
* Download the repository
* Let your IDE import necessary dependencies automatically or fetch them manually
* For local repository publication run the following tasks
  * `gradlew :cqp-api:build :cqp-api:publishToMavenLocal`
  * `gradle build -x :cqp-api:build`
  * `gradle publishToMavenLocal -x :cqp-api:publishToMavenLocal`

## Repository configuration

* The shared plugin reads the Akka repository URL in this order and stops at the first match:
  1. Gradle property `AKKA_REPO_URL` (set via `gradle.properties` or `-PAKKA_REPO_URL=...`)
  2. Gradle property `akkaRepoUrl` (legacy camelCase fallback)
  3. Environment variable `AKKA_REPO_URL`
* If none of the above is supplied, the build fails immediately. Obtain the secure URL/token from https://account.akka.io/ and set one of the properties/variables before running Gradle.
