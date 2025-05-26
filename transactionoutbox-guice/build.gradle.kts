dependencies {
    api(project(":transactionoutbox-core"))
    api(libs.com.google.inject.guice)
    testImplementation(libs.com.google.guava.guava)
    testImplementation(libs.ch.qos.logback.logback.classic)
    testImplementation(libs.ch.qos.logback.logback.core)
    testImplementation(libs.org.hamcrest.hamcrest.core)
    testImplementation(libs.org.junit.jupiter.junit.jupiter.engine)
    testImplementation(libs.org.mockito.mockito.core)
    compileOnly(libs.org.projectlombok.lombok)
}

description = "Transaction Outbox Guice"
