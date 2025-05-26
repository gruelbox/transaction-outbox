plugins {
    alias(libs.plugins.lombok)
}

dependencies {
    api(project(":transactionoutbox-core"))
    api(platform(libs.com.fasterxml.jackson.jackson.bom))
    api(libs.com.fasterxml.jackson.core.jackson.databind)
    api(libs.org.apache.commons.commons.lang3)
    
    compileOnly(libs.org.projectlombok.lombok)

    testImplementation(project(":transactionoutbox-testing"))
    testImplementation(libs.com.fasterxml.jackson.datatype.jackson.datatype.guava)
    testImplementation(libs.com.fasterxml.jackson.datatype.jackson.datatype.jdk8)
    testImplementation(libs.com.fasterxml.jackson.datatype.jackson.datatype.jsr310)
    testImplementation(libs.com.google.guava.guava)
    testImplementation(libs.com.h2database.h2)
}

description = "Transaction Outbox Jackson Support"
