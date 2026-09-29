package com.gruelbox.transactionoutbox.spi;

import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;

/**
 * A prepared statement which counts the rows added to its batch since the batch was last executed,
 * so that a transaction can tell which of its statements have anything left to send.
 */
public interface BatchCountingStatement extends PreparedStatement {

  /**
   * @return The number of rows added to the batch and not yet executed.
   */
  int getBatchCount();

  /**
   * @param delegate The statement to count the batches of.
   * @return A statement which passes everything on to the delegate, counting as it goes.
   */
  static BatchCountingStatement countBatches(PreparedStatement delegate) {
    return (BatchCountingStatement)
        Proxy.newProxyInstance(
            BatchCountingStatement.class.getClassLoader(),
            new Class[] {BatchCountingStatement.class},
            new BatchCounter(delegate));
  }
}
