plugins {
    alias(libs.plugins.lombok)
}

dependencies {
    api(project(":transactionoutbox-core"))
    api(libs.org.slf4j.slf4j.api)
    
    compileOnly(libs.org.projectlombok.lombok)
    
    testImplementation(project(":transactionoutbox-core"))
    testImplementation(project(":transactionoutbox-jooq"))
    testImplementation(project(":transactionoutbox-testing"))
    testImplementation(platform(libs.org.testcontainers.testcontainers.bom))
    testImplementation(libs.ch.qos.logback.logback.classic)
    testImplementation(libs.ch.qos.logback.logback.core)
    testImplementation(libs.com.h2database.h2)
    testImplementation(libs.com.microsoft.sqlserver.mssql.jdbc)
    testImplementation(libs.com.mysql.mysql.connector.j)
    testImplementation(libs.com.oracle.database.jdbc.ojdbc11)
    testImplementation(libs.me.escoffier.loom.loom.unit)
    testImplementation(libs.org.mockito.mockito.core)
    testImplementation(libs.org.postgresql.postgresql)
    testImplementation(libs.org.testcontainers.junit.jupiter)
    testImplementation(libs.org.testcontainers.mssqlserver)
    testImplementation(libs.org.testcontainers.mysql)
    testImplementation(libs.org.testcontainers.oracle.xe)
    testImplementation(libs.org.testcontainers.postgresql)
    testImplementation(libs.org.testcontainers.testcontainers)
}

description = "Transaction Outbox Virtual Threads Support" 