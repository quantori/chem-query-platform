## Build instructions
* Download the repository
* Let your IDE import necessary dependencies automatically or fetch them manually
* For local repository publication Run `gradle build` task and then run `gradle publishToMavenLocal` tasks

## Repository configuration

* Set the `AKKA_REPO_URL` Gradle property or environment variable before running builds. The shared plugin fails fast when the URL is absent to avoid silently compiling without the commercial Akka repository.
