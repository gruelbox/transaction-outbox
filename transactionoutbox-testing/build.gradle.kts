dependencies {
    api(project(":transactionoutbox-core"))
    api(libs.ch.qos.logback.logback.classic)
    api(libs.ch.qos.logback.logback.core)
    api(libs.org.hamcrest.hamcrest.core)
    api(libs.org.junit.jupiter.junit.jupiter.engine)
    api(libs.org.junit.jupiter.junit.jupiter.api)
    api(libs.org.junit.jupiter.junit.jupiter.params)
    api(libs.org.mockito.mockito.core)
    api(libs.com.zaxxer.hikaricp)
    compileOnly(libs.org.projectlombok.lombok)
}

description = "Transaction Outbox Testing"
