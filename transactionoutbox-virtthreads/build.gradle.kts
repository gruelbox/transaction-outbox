dependencies {
    api(project(":transactionoutbox-core"))
    api(libs.org.slf4j.slf4j.api)
    
    // Test dependencies
    testImplementation(project(":transactionoutbox-core"))
    testImplementation(project(":transactionoutbox-testing"))
    testImplementation(project(":transactionoutbox-jooq"))
    testImplementation(libs.me.escoffier.loom.loom.unit)
    testImplementation(libs.org.testcontainers.testcontainers)
    testImplementation(libs.org.testcontainers.junit.jupiter)
    testImplementation(libs.org.testcontainers.postgresql)
    testImplementation(libs.org.testcontainers.oracle.xe)
    testImplementation(libs.org.testcontainers.mysql)
    testImplementation(libs.org.postgresql.postgresql)
    testImplementation(libs.com.oracle.database.jdbc.ojdbc11)
    testImplementation(libs.com.mysql.mysql.connector.j)
    testImplementation(libs.com.h2database.h2)
    testImplementation(libs.org.testcontainers.mssqlserver)
    testImplementation(libs.com.microsoft.sqlserver.mssql.jdbc)
    testImplementation(libs.org.junit.jupiter.junit.jupiter.engine)
    testImplementation(libs.org.junit.jupiter.junit.jupiter.params)
    testImplementation(libs.org.mockito.mockito.core)
    testImplementation(libs.ch.qos.logback.logback.classic)
    testImplementation(libs.ch.qos.logback.logback.core)
}

description = "Transaction Outbox Virtual Threads Support" 