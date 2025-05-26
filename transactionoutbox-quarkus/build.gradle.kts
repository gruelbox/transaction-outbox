
dependencies {
    api(project(":transactionoutbox-core"))
    api(libs.jakarta.enterprise.jakarta.enterprise.cdi.api)
    api(libs.jakarta.transaction.jakarta.transaction.api)
    testImplementation(libs.io.quarkus.quarkus.junit5)
    testImplementation(libs.io.quarkus.quarkus.resteasy)
    testImplementation(libs.io.quarkus.quarkus.arc)
    testImplementation(libs.io.quarkus.quarkus.jdbc.h2)
    testImplementation(libs.io.quarkus.quarkus.agroal)
    testImplementation(libs.io.quarkus.quarkus.undertow)
}

description = "Transaction Outbox Quarkus"
