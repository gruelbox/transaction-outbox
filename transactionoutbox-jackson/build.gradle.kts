
dependencies {
    api(project(":transactionoutbox-core"))
    api(libs.com.fasterxml.jackson.core.jackson.databind)
    api(libs.org.apache.commons.commons.lang3)
    testImplementation(project(":transactionoutbox-testing"))
    testImplementation(libs.com.fasterxml.jackson.datatype.jackson.datatype.guava)
    testImplementation(libs.com.google.guava.guava)
    testImplementation(libs.com.fasterxml.jackson.datatype.jackson.datatype.jdk8)
    testImplementation(libs.com.fasterxml.jackson.datatype.jackson.datatype.jsr310)
    testImplementation(libs.com.h2database.h2)
    compileOnly(libs.org.projectlombok.lombok)
}

description = "Transaction Outbox Jackson"
