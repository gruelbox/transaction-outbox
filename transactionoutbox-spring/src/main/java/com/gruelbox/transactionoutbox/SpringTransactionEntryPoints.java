package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier;
import lombok.AllArgsConstructor;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Spring AOP weavable transaction entry points. Only public to make use of Spring AOP; do not
 * reference directly.
 */
@SuppressWarnings("WeakerAccess")
@AllArgsConstructor
@NotApi
public class SpringTransactionEntryPoints {
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, SpringTransaction> work, SpringTransaction transaction)
      throws E {
    return work.doWork(transaction);
  }
}
