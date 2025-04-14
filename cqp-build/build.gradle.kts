plugins {
    `kotlin-dsl`
}

group = "com.quantori.cqp-build"

repositories {
    mavenLocal()
    mavenCentral()
}

gradlePlugin {
    plugins {
        create("cqpJavaLibrary") {
            id = "com.quantori.cqp-build"
            implementationClass = "com.quantori.cqp.build.CqpJavaLibraryPlugin"
        }
    }
}

dependencies {
    compileOnly(gradleApi())
}
