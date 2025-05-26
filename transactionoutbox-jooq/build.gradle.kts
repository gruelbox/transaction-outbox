
dependencies {
    api(project(":transactionoutbox-core"))
    api(libs.org.jooq.jooq)
    testImplementation(project(":transactionoutbox-testing"))
    testImplementation(libs.com.h2database.h2)
    testImplementation(libs.org.testcontainers.testcontainers)
    testImplementation(libs.org.testcontainers.junit.jupiter)
    testImplementation(libs.org.testcontainers.postgresql)
    testImplementation(libs.org.testcontainers.oracle.xe)
    testImplementation(libs.org.testcontainers.mysql)
    testImplementation(libs.org.postgresql.postgresql)
    testImplementation(libs.com.oracle.database.jdbc.ojdbc11)
    testImplementation(libs.com.mysql.mysql.connector.j)
    testImplementation(libs.org.testcontainers.mssqlserver)
    testImplementation(libs.com.microsoft.sqlserver.mssql.jdbc)
    compileOnly(libs.org.projectlombok.lombok)
}

description = "Transaction Outbox jOOQ"
