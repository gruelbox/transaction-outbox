package com.gruelbox.transactionoutbox.spring;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.verify;

import com.gruelbox.transactionoutbox.UncheckedException;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;

@ExtendWith(MockitoExtension.class)
class SpringTransactionManagerTest {

  @Mock private PlatformTransactionManager platformTransactionManager;

  @Mock private DataSource dataSource;

  @Mock private TransactionStatus transactionStatus;
  @Mock private SpringTransactionManager springTransactionManager;

  @BeforeEach
  void setUp() {
    Mockito.when(platformTransactionManager.getTransaction(Mockito.any()))
        .thenReturn(transactionStatus);
    springTransactionManager = new SpringTransactionManager(platformTransactionManager, dataSource);
  }

  private static class MyRuntimeException extends RuntimeException {}

  private static class MyCheckedException extends Exception {}

  private static class MyUncheckedException extends UncheckedException {

    public MyUncheckedException(Throwable cause) {
      super(cause);
    }
  }

  private static class MySpringTransactionException extends TransactionException {

    public MySpringTransactionException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  private static class MySqlException extends SQLException {}

  @Test
  void shouldWorkInNewTransactionAndCommit() {
    springTransactionManager.inTransactionReturnsThrows(transaction -> true);

    verify(platformTransactionManager)
        .getTransaction(
            ArgumentMatchers.assertArg(
                trxDef -> {
                  Assertions.assertThat(trxDef).isNotNull();
                  Assertions.assertThat(trxDef.getPropagationBehavior())
                      .isEqualTo(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
                }));
    verify(platformTransactionManager).commit(same(transactionStatus));
  }

  @Test
  void shouldRollbackOnFailure() {
    assertThrows(
        RuntimeException.class,
        () -> {
          springTransactionManager.inTransactionReturnsThrows(
              transaction -> {
                throw new RuntimeException();
              });
        });

    verify(platformTransactionManager)
        .getTransaction(
            ArgumentMatchers.assertArg(
                trxDef -> {
                  Assertions.assertThat(trxDef).isNotNull();
                  Assertions.assertThat(trxDef.getPropagationBehavior())
                      .isEqualTo(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
                }));
    verify(platformTransactionManager).rollback(same(transactionStatus));
  }

  @Test
  void shouldPreserveRuntimeException() {
    assertThrows(
        MyRuntimeException.class,
        () -> {
          springTransactionManager.inTransactionReturnsThrows(
              transaction -> {
                throw new MyRuntimeException();
              });
        });
  }

  @Test
  void shouldPreserveCheckedException() {
    assertThrows(
        MyCheckedException.class,
        () -> {
          springTransactionManager.inTransactionReturnsThrows(
              transaction -> {
                throw new MyCheckedException();
              });
        });
  }

  @Test
  void shouldPreserveUncheckedException() {
    assertThrows(
        MyUncheckedException.class,
        () -> {
          springTransactionManager.inTransactionReturnsThrows(
              transaction -> {
                throw new MyUncheckedException(new RuntimeException());
              });
        });
  }

  @Test
  void shouldPreserveSpringTransactionException() {
    assertThrows(
        MySpringTransactionException.class,
        () -> {
          springTransactionManager.inTransactionReturnsThrows(
              transaction -> {
                throw new MySpringTransactionException("expected failure", new RuntimeException());
              });
        });
  }

  @Test
  void shouldPreserveSqlException() {
    assertThrows(
        MySqlException.class,
        () -> {
          springTransactionManager.inTransactionReturnsThrows(
              transaction -> {
                throw new MySqlException();
              });
        });
  }
}
