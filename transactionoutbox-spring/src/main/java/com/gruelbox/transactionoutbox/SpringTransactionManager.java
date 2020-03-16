package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.util.Optional;
import java.util.concurrent.Callable;
import javax.persistence.EntityManager;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.hibernate.Session;
import org.hibernate.internal.SessionImpl;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/** Transaction manager which uses spring-tx and Hibernate. */
@Slf4j
@SuperBuilder
public class SpringTransactionManager extends BaseTransactionManager {

  /** The Hibernate entity manager. */
  @NonNull private final EntityManager entityManager;

  @Override
  public Connection getActiveConnection() {
    return Optional.ofNullable(((SessionImpl) entityManager.getDelegate()).connection())
        .orElseThrow(NoTransactionActiveException::new);
  }

  @Override
  public final <T> T inTransactionReturnsThrows(Callable<T> callable) {
    pushLists();
    try {
      var result = inTransaction(connection -> {
        try {
          T out = callable.call();
          flushBatches();
          return out;
        } finally {
          closeBatchStatements();
        }
      });
      processHooks();
      return result;
    } finally {
      popLists();
    }
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  private <T> T inTransaction(ThrowingFunction<Connection, T> work) {
    return ((Session) entityManager.getDelegate()).doReturningWork(connection -> {
      try {
        return work.apply(connection);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
