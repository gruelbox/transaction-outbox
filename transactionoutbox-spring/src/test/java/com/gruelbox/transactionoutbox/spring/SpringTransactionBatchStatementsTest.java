package com.gruelbox.transactionoutbox.spring;

import static com.gruelbox.transactionoutbox.spi.Utils.uncheck;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gruelbox.transactionoutbox.ThrowingTransactionalWork;
import com.gruelbox.transactionoutbox.Transaction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * {@link Transaction#prepareBatchStatement(String)} promises a statement that is cached and re-used
 * within a transaction, so that every row added to it is sent in one batch just before the commit.
 */
class SpringTransactionBatchStatementsTest {

  private static final String INSERT = "INSERT INTO OUTBOX (id) VALUES (?)";
  private static final String UPDATE = "UPDATE OUTBOX SET attempts = ? WHERE id = ?";

  private final DataSource dataSource = mock(DataSource.class);
  private final Connection connection = mock(Connection.class);
  private final PreparedStatement insert = mock(PreparedStatement.class);
  private final PreparedStatement update = mock(PreparedStatement.class);
  private final DataSourceTransactionManager platformTransactionManager =
      new DataSourceTransactionManager(dataSource);
  private final SpringTransactionManager transactionManager =
      new SpringTransactionManager(platformTransactionManager, dataSource);
  private final TransactionTemplate springTransaction =
      new TransactionTemplate(platformTransactionManager);

  @BeforeEach
  void setUp() throws SQLException {
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.prepareStatement(INSERT)).thenReturn(insert);
    when(connection.prepareStatement(UPDATE)).thenReturn(update);
  }

  @Test
  void shouldReuseOneStatementPerSqlWithinATransaction() throws SQLException {
    inTransaction(
        tx ->
            assertThat(tx.prepareBatchStatement(INSERT))
                .isSameAs(tx.prepareBatchStatement(INSERT)));

    verify(connection, times(1)).prepareStatement(INSERT);
  }

  @Test
  void shouldSendEveryRowAddedInOneBatchBeforeTheCommit() throws SQLException {
    inTransaction(
        tx -> {
          addRow(tx, INSERT);
          addRow(tx, INSERT);
          addRow(tx, INSERT);
        });

    verify(insert, times(3)).addBatch();
    verify(insert, times(1)).executeBatch();
    var order = inOrder(insert, connection);
    order.verify(insert).executeBatch();
    order.verify(connection).commit();
  }

  @Test
  void shouldKeepStatementsForDifferentSqlSeparate() throws SQLException {
    inTransaction(
        tx -> {
          addRow(tx, INSERT);
          addRow(tx, UPDATE);
          addRow(tx, INSERT);
        });

    verify(connection, times(1)).prepareStatement(INSERT);
    verify(connection, times(1)).prepareStatement(UPDATE);
    verify(insert, times(1)).executeBatch();
    verify(update, times(1)).executeBatch();
  }

  @Test
  void shouldSendNothingWhenNoRowWasAdded() throws SQLException {
    inTransaction(tx -> tx.prepareBatchStatement(INSERT));

    verify(insert, never()).executeBatch();
    verify(insert).close();
  }

  @Test
  void shouldCloseTheStatementAndSkipTheBatchOnRollback() throws SQLException {
    springTransaction.executeWithoutResult(
        status -> {
          transactionManager.requireTransaction(tx -> addRow(tx, INSERT));
          status.setRollbackOnly();
        });

    verify(insert, never()).executeBatch();
    verify(insert).close();
    verify(connection).rollback();
  }

  @Test
  void shouldPrepareAFreshStatementInTheNextTransaction() throws SQLException {
    inTransaction(tx -> addRow(tx, INSERT));
    inTransaction(tx -> addRow(tx, INSERT));

    verify(connection, times(2)).prepareStatement(INSERT);
    verify(insert, times(2)).executeBatch();
  }

  @Test
  void shouldSendTheBatchOnFlushAndNotAgainAtCommit() throws SQLException {
    inTransaction(
        tx -> {
          addRow(tx, INSERT);
          addRow(tx, INSERT);
          tx.flushBatches();
          uncheck(() -> verify(insert, times(1)).executeBatch());
        });

    verify(insert, times(1)).executeBatch();
  }

  @Test
  void shouldSendRowsAddedAfterAFlushAtCommit() throws SQLException {
    inTransaction(
        tx -> {
          addRow(tx, INSERT);
          tx.flushBatches();
          addRow(tx, INSERT);
          addRow(tx, INSERT);
        });

    verify(insert, times(3)).addBatch();
    var order = inOrder(insert, connection);
    order.verify(insert).executeBatch();
    order.verify(insert).executeBatch();
    order.verify(connection).commit();
  }

  @Test
  void shouldHaveNothingToFlushWhenNoStatementWasPrepared() throws SQLException {
    inTransaction(Transaction::flushBatches);

    verify(connection, never()).prepareStatement(anyString());
  }

  /** Spring suspends the outer transaction's connection, so its statements must stay with it. */
  @Test
  void shouldNotShareStatementsWithAnInnerTransactionOnAnotherConnection() throws SQLException {
    var innerConnection = mock(Connection.class);
    var innerInsert = mock(PreparedStatement.class);
    when(dataSource.getConnection()).thenReturn(connection, innerConnection);
    when(innerConnection.prepareStatement(INSERT)).thenReturn(innerInsert);
    var separateSpringTransaction =
        new TransactionTemplate(
            platformTransactionManager,
            new DefaultTransactionDefinition(TransactionDefinition.PROPAGATION_REQUIRES_NEW));

    springTransaction.executeWithoutResult(
        outer -> {
          transactionManager.requireTransaction(tx -> addRow(tx, INSERT));
          separateSpringTransaction.executeWithoutResult(
              inner -> transactionManager.requireTransaction(tx -> addRow(tx, INSERT)));
        });

    var order = inOrder(innerInsert, innerConnection, insert, connection);
    order.verify(innerInsert).executeBatch();
    order.verify(innerConnection).commit();
    order.verify(insert).executeBatch();
    order.verify(connection).commit();
  }

  private void inTransaction(ThrowingTransactionalWork<RuntimeException> work) {
    springTransaction.executeWithoutResult(status -> transactionManager.requireTransaction(work));
  }

  private static void addRow(Transaction tx, String sql) {
    uncheck(() -> tx.prepareBatchStatement(sql).addBatch());
  }
}
