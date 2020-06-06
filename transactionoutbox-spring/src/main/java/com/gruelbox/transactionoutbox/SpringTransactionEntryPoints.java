package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier;
import lombok.AllArgsConstructor;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Spring AOP weavable transaction entry points.
 */
@SuppressWarnings("WeakerAccess")
@AllArgsConstructor
public class SpringTransactionEntryPoints {

  @Transactional(propagation = Propagation.MANDATORY)
  public <T, E extends Exception> T requireTransactionReturns(
      com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier<T, E, SpringTransaction> work, SpringTransaction transaction)
      throws E, NoTransactionActiveException {
    return work.doWork(transaction);
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, SpringTransaction> work, SpringTransaction transaction) throws E {
    return work.doWork(transaction);
  }

}
