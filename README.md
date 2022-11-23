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
./gradlew build
`

And finally:

`
gradle publishToMavenLocal
`

## Tests
To make tests working it needs to login to AWS ECR. To do this simply run the following command

```
export AWS_ACCESS_KEY_ID="Your Access Key"
export AWS_SECRET_ACCESS_KEY="Your Access Key"
export AWS_DEFAULT_REGION=us-east-2
export AWS_ACCOUNT_ID="Your Account ID"
export AWS_URI=$AWS_ACCOUNT_ID.dkr.ecr.$AWS_DEFAULT_REGION.amazonaws.com

aws ecr get-login-password --region $AWS_DEFAULT_REGION | docker login --username AWS --password-stdin $AWS_URI
```