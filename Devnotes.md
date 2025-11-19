## Build instructions
* Download the repository
* Let your IDE import necessary dependencies automatically or fetch them manually
* For local repository publication run the following tasks
  * `gradlew :cqp-api:build :cqp-api:publishToMavenLocal`
  * `gradle build -x :cqp-api:build`
  * `gradle publishToMavenLocal -x :cqp-api:publishToMavenLocal`

## Repository configuration

* Set the `AKKA_REPO_URL` Gradle property or environment variable before running builds. The shared plugin fails fast when the URL is absent to avoid silently compiling without the commercial Akka repository. This URL should contain special security token which can be created here- https://account.akka.io/.
