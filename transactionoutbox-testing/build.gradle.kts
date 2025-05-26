plugins {
    alias(libs.plugins.lombok)
}

dependencies {
    api(project(":transactionoutbox-core"))
    api(platform(libs.org.junit.bom))
    api(libs.ch.qos.logback.logback.classic)
    api(libs.ch.qos.logback.logback.core)
    api(libs.com.zaxxer.hikaricp)
    api(libs.org.hamcrest.hamcrest.core)
    api(libs.org.junit.jupiter.junit.jupiter.api)
    api(libs.org.junit.jupiter.junit.jupiter.engine)
    api(libs.org.junit.jupiter.junit.jupiter.params)
    api(libs.org.mockito.mockito.core)

    compileOnly(libs.org.projectlombok.lombok)
    
    runtimeOnly(libs.org.junit.platform.junit.platform.launcher)
}

description = "Transaction Outbox Testing"
