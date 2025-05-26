plugins {
    alias(libs.plugins.lombok)
}

dependencies {
    api(libs.com.google.code.gson.gson)
    api(libs.net.bytebuddy.byte.buddy)
    api(libs.org.objenesis.objenesis)
    api(libs.org.slf4j.slf4j.api)
    
    compileOnly(libs.org.projectlombok.lombok)

    testImplementation(platform(libs.org.junit.bom))
    testImplementation(libs.ch.qos.logback.logback.classic)
    testImplementation(libs.ch.qos.logback.logback.core)
    testImplementation(libs.com.h2database.h2)
    testImplementation(libs.com.zaxxer.hikaricp)
    testImplementation(libs.org.hamcrest.hamcrest.core)
    testImplementation(libs.org.junit.jupiter.junit.jupiter.engine)
    testImplementation(libs.org.junit.jupiter.junit.jupiter.params)
    testImplementation(libs.org.mockito.mockito.core)
    testRuntimeOnly(libs.org.junit.platform.junit.platform.launcher)
}

description = "Transaction Outbox Core"
