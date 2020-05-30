package com.gruelbox.transactionoutbox;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.ea.async.Async;
import com.gruelbox.transactionoutbox.jdbc.JdbcTransactionManager;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcTransactionManager;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Test;

/**
 * Some temporary rules to enforce modularity during refactoring, before this stuff is all hauled
 * into separate modules.
 */
class TestGlobalArchitecture {

  private static final JavaClasses all =
      new ClassFileImporter().importPackagesOf(TransactionManager.class);

  @Test
  void no_r2dbc_access_to_jdbc() {
    classes()
        .that()
        .resideInAPackage(R2dbcTransactionManager.class.getPackageName())
        .should()
        .onlyAccessClassesThat()
        .resideOutsideOfPackages(JdbcTransactionManager.class.getPackageName(), "java.sql..")
        .check(all);
  }

  @Test
  void no_jdbc_access_to_r2dbc() {
    classes()
        .that()
        .resideInAPackage(JdbcTransactionManager.class.getPackageName())
        .should()
        .onlyAccessClassesThat()
        .resideOutsideOfPackages(R2dbcTransactionManager.class.getPackageName(), "io.r2dbc..")
        .check(all);
  }

  @Test
  void no_core_access_to_jdbc_or_r2dbc() {
    classes()
        .that()
        .resideInAPackage(TransactionManager.class.getPackageName())
        .should()
        .onlyAccessClassesThat()
        .resideOutsideOfPackages(
            JdbcTransactionManager.class.getPackageName(),
            R2dbcTransactionManager.class.getPackageName(),
            "java.sql..",
            "io.r2dbc..")
        .check(all);
  }

  @Test
  void no_core_use_of_reactor() {
    classes()
        .that()
        .resideInAPackage(TransactionManager.class.getPackageName())
        .should()
        .onlyAccessClassesThat()
        .resideOutsideOfPackage("reactor.core..")
        .check(all);
  }

  @Test
  void no_use_of_async_await() {
    classes()
        .should()
        .onlyAccessClassesThat()
        .resideOutsideOfPackage(Async.class.getPackageName())
        .check(all);
  }
}
