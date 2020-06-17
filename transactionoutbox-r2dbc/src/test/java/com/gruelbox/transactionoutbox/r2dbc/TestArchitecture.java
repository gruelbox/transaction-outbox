package com.gruelbox.transactionoutbox.r2dbc;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.gruelbox.transactionoutbox.jdbc.JdbcPersistor;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import org.junit.jupiter.api.Test;

/**
 * Some temporary rules to enforce modularity during refactoring, before this stuff is all hauled
 * into separate modules.
 */
class TestArchitecture {

  private static final JavaClasses all =
      new ClassFileImporter().importPackagesOf(R2dbcTransactionManager.class);

  @Test
  void no_r2dbc_access_to_jdbc() {
    classes()
        .that()
        .resideInAPackage(R2dbcTransactionManager.class.getPackageName())
        .should()
        .onlyAccessClassesThat()
        .resideOutsideOfPackages(JdbcPersistor.class.getPackageName(), "java.sql..")
        .check(all);
  }
}
