package com.gruelbox.transactionoutbox;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Beta
@Slf4j
@Service
public class SpringTransactionManagerImpl implements SpringTransactionManager {

  @PersistenceContext private EntityManager entityManager;

  private volatile boolean initialized;
  private SpringTransaction transaction;

  private SpringTransaction transaction() {
    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          initialized = true;
          SpringTransaction tx = new SpringTransaction(entityManager);
          transaction = tx;
          return tx;
        }
      }
    }
    return transaction;
  }

  @Override
  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public <T, E extends Exception> T inTransactionReturnsThrows(
      ThrowingTransactionalSupplier<T, E, SpringTransaction> work) throws E {
    return work.doWork(transaction());
  }

  @Override
  @Transactional(propagation = Propagation.MANDATORY)
  public <T, E extends Exception> T requireTransactionReturns(
      ThrowingTransactionalSupplier<T, E, SpringTransaction> work)
      throws E, NoTransactionActiveException {
    return work.doWork(transaction());
  }
}
