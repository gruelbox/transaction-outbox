plugins {
    alias(libs.plugins.lombok)
}

dependencies {
    api(project(":transactionoutbox-core"))
    api(platform(libs.org.springframework.boot.bom))
    api(platform(libs.org.springframework.spring.framework.bom))
    api(libs.org.springframework.spring.context)
    api(libs.org.springframework.spring.tx)
    
    compileOnly(libs.org.projectlombok.lombok)
    compileOnly(libs.org.springframework.spring.jdbc)
    
    testImplementation(project(":transactionoutbox-jackson"))
    testImplementation(project(":transactionoutbox-testing"))
    testImplementation(libs.ch.qos.logback.logback.classic)
    testImplementation(libs.ch.qos.logback.logback.core)
    testImplementation(libs.com.h2database.h2)
    testImplementation(libs.com.zaxxer.hikaricp)
    testImplementation(libs.org.awaitility.awaitility)
    testImplementation(libs.org.springframework.boot.spring.boot.starter.data.jpa)
    testImplementation(libs.org.springframework.boot.spring.boot.starter.jdbc)
    testImplementation(libs.org.springframework.boot.spring.boot.starter.test)
    testImplementation(libs.org.springframework.boot.spring.boot.starter.web)
}

description = "Transaction Outbox Spring"
