
dependencies {
    api(project(":transactionoutbox-core"))
    testImplementation(project(":transactionoutbox-jackson"))
    testImplementation(libs.ch.qos.logback.logback.classic)
    testImplementation(libs.ch.qos.logback.logback.core)
    testImplementation(libs.com.h2database.h2)
    testImplementation(libs.com.zaxxer.hikaricp)
    testImplementation(libs.org.springframework.boot.spring.boot.starter.data.jpa)
    testImplementation(libs.org.springframework.boot.spring.boot.starter.web)
    testImplementation(libs.org.springframework.boot.spring.boot.starter.test)
    testImplementation(libs.org.junit.jupiter.junit.jupiter.engine)
    testImplementation(libs.org.junit.jupiter.junit.jupiter.api)
    testImplementation(libs.org.junit.jupiter.junit.jupiter.params)
    testImplementation(libs.org.awaitility.awaitility)
    compileOnly(libs.org.springframework.spring.tx)
    compileOnly(libs.org.springframework.spring.jdbc)
    compileOnly(libs.org.springframework.spring.context)
    compileOnly(libs.org.projectlombok.lombok)
}

description = "Transaction Outbox Spring"
