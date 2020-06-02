package com.gruelbox.transactionoutbox;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransactionManager;
import com.gruelbox.transactionoutbox.sql.SqlPersistor;
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
  void no_core_access_to_sql_or_jdbc() {
    classes()
        .that()
        .resideInAPackage(TransactionManager.class.getPackageName())
        .should()
        .onlyAccessClassesThat()
        .resideOutsideOfPackages(
            JdbcTransactionManager.class.getPackageName(),
            SqlPersistor.class.getPackageName(),
            "java.sql..")
        .check(all);
  }
}
