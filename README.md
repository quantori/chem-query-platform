# prj-bik-qdp

## Build
First, you need to copy `build.gradle` file to the project root.
Currently, we don't store the file in repository, please ask your teammates.

Now you can execute:

`
gradle wrapper
`

After that:

`
./gradlew build -x test
`

And finally:

`
gradle publishToMavenLocal
`