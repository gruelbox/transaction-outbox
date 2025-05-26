import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    `java-library`
    `maven-publish`
    alias(libs.plugins.lombok)
}

allprojects {
    repositories {
        mavenLocal()
        maven {
            url = uri("https://repo.maven.apache.org/maven2/")
        }
    }
    group = providers.gradleProperty("customGroup").getOrElse("com.gruelbox")
    version = providers.gradleProperty("customVersion").getOrElse("1.3.99999-SNAPSHOT")
}

val testFixturesModules = listOf(
    "transactionoutbox-testing"
)

val java21Modules = listOf(
    "transactionoutbox-jooq",
    "transactionoutbox-virtthreads",
)

val java17Modules = listOf(
    "transactionoutbox-quarkus",
    "transactionoutbox-spring",
)

subprojects {
    apply(plugin = "java-library")
    if (testFixturesModules.contains(project.name)) {
        apply(plugin = "java-test-fixtures")
    }
    apply(plugin = "maven-publish")

    val javaVersion = when {
        project.name in java21Modules -> JavaVersion.VERSION_21
        project.name in java17Modules -> JavaVersion.VERSION_17
        else -> JavaVersion.VERSION_11
    }

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(javaVersion.toString()))
        }
    }

    afterEvaluate {
        publishing {
            publications.create<MavenPublication>(project.name) {
                from(project.components["java"])
                pom.withXml {}

                versionMapping {
                    allVariants {
                        fromResolutionResult()
                    }
                }
            }
        }
    }

    tasks.withType<JavaCompile> {
        options.encoding = "UTF-8"
    }

    tasks.withType<Javadoc> {
        options.encoding = "UTF-8"
    }

    tasks.withType<Test> {
        useJUnitPlatform()

        testLogging {
            displayGranularity = 1
            showCauses = true
            showStackTraces = true
            exceptionFormat = TestExceptionFormat.FULL
            events(TestLogEvent.PASSED, TestLogEvent.SKIPPED, TestLogEvent.FAILED, TestLogEvent.STANDARD_ERROR)
        }

        outputs.upToDateWhen { false }
        outputs.cacheIf { false }
    }
} 