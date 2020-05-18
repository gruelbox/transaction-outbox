package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.util.Arrays;
import java.util.stream.Collectors;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

/** A stub transaction manager that assumes no underlying database. */
@SuperBuilder
@Slf4j
public class StubTransactionManager
    extends AbstractThreadLocalTransactionManager<SimpleTransaction> {

  protected StubTransactionManager() {}

  @Override
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E> work) throws E {
    return withTransaction(
        atx -> {
          T result = work.doWork(atx);
          ((SimpleTransaction) atx).processHooks();
          return result;
        });
  }

  private <T, E extends Exception> T withTransaction(ThrowingTransactionalSupplier<T, E> work)
      throws E {
    Connection mockConnection =
        Utils.createProxy(
            Connection.class,
            (method, args) -> {
              log.info(
                  "Called mock Connection.{}({})",
                  method.getName(),
                  args == null
                      ? ""
                      : Arrays.stream(args)
                          .map(it -> it == null ? "null" : it.toString())
                          .collect(Collectors.joining(", ")));
              return null;
            });
    try (SimpleTransaction transaction =
        pushTransaction(new SimpleTransaction(mockConnection, null))) {
      return work.doWork(transaction);
    } finally {
      popTransaction();
    }
  }
}
