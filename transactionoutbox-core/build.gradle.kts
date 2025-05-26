dependencies {
    api(libs.org.slf4j.slf4j.api)
    api(libs.net.bytebuddy.byte.buddy)
    api(libs.org.objenesis.objenesis)
    api(libs.com.google.code.gson.gson)
    testImplementation(libs.org.hamcrest.hamcrest.core)
    testImplementation(libs.org.junit.jupiter.junit.jupiter.engine)
    testImplementation(libs.org.junit.jupiter.junit.jupiter.params)
    testImplementation(libs.org.mockito.mockito.core)
    testImplementation(libs.ch.qos.logback.logback.classic)
    testImplementation(libs.ch.qos.logback.logback.core)
    testImplementation(libs.com.zaxxer.hikaricp)
    testImplementation(libs.com.h2database.h2)
    compileOnly(libs.org.projectlombok.lombok)
}

description = "Transaction Outbox Core"
