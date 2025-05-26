plugins {
    alias(libs.plugins.lombok)
}

dependencies {
    api(project(":transactionoutbox-core"))
    api(libs.com.google.inject.guice)
    
    compileOnly(libs.org.projectlombok.lombok)
    
    testImplementation(platform(libs.org.junit.bom))
    testImplementation(libs.ch.qos.logback.logback.classic)
    testImplementation(libs.ch.qos.logback.logback.core)
    testImplementation(libs.com.google.guava.guava)
    testImplementation(libs.org.hamcrest.hamcrest.core)
    testImplementation(libs.org.junit.jupiter.junit.jupiter.engine)
    testImplementation(libs.org.mockito.mockito.core)
    testRuntimeOnly(libs.org.junit.platform.junit.platform.launcher)
}

description = "Transaction Outbox Guice"
