package com.gruelbox.transactionoutbox.jooq.acceptance;

import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.testing.AbstractAcceptanceTest;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;

abstract class AbstractJooqAcceptanceTest extends AbstractAcceptanceTest {

  protected DSLContext dsl;

  @Override
  protected TransactionManager txManager() {
    throw new IllegalStateException("Needs to be defined");
  }

  @Test
  abstract void testNestedDirectInvocation() throws Exception;

  @Test
  abstract void testNestedViaListener() throws Exception;

  @Test
  abstract void testNestedWithInnerFailure() throws Exception;
}
