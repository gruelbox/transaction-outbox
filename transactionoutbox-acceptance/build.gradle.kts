plugins {
    alias(libs.plugins.lombok)
}

dependencies {
    testImplementation(project(":transactionoutbox-core"))
    testImplementation(project(":transactionoutbox-testing"))
    testImplementation(platform(libs.org.junit.bom))
    testImplementation(platform(libs.org.testcontainers.testcontainers.bom))
    testImplementation(libs.com.h2database.h2)
    testImplementation(libs.com.microsoft.sqlserver.mssql.jdbc)
    testImplementation(libs.com.mysql.mysql.connector.j)
    testImplementation(libs.com.oracle.database.jdbc.ojdbc11)
    testImplementation(libs.org.postgresql.postgresql)
    testImplementation(libs.org.testcontainers.junit.jupiter)
    testImplementation(libs.org.testcontainers.mssqlserver)
    testImplementation(libs.org.testcontainers.mysql)
    testImplementation(libs.org.testcontainers.oracle.xe)
    testImplementation(libs.org.testcontainers.postgresql)
    testImplementation(libs.org.testcontainers.testcontainers)
}

description = "Transaction Outbox Acceptance Tests"
