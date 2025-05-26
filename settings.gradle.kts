pluginManagement {
    repositories {
        gradlePluginPortal()
    }
    plugins {
        id("org.gradle.toolchains.foojay-resolver-convention") version "0.10.0"
    }
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.10.0"
}

rootProject.name = "transaction-outbox"

include(":transactionoutbox-jackson")
include(":transactionoutbox-testing")
include(":transactionoutbox-quarkus")
include(":transactionoutbox-spring")
include(":transactionoutbox-guice")
include(":transactionoutbox-core")
include(":transactionoutbox-acceptance")
include(":transactionoutbox-jooq")
include(":transactionoutbox-virtthreads")
