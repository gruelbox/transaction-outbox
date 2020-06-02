package com.gruelbox.transactionoutbox.jdbc;

import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.PersistorWrapper;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.sql.Dialect;
import com.gruelbox.transactionoutbox.sql.SqlPersistor;
import java.sql.Connection;
import java.util.concurrent.CompletableFuture;

/**
 * The default JDBC-based {@link Persistor} for {@link TransactionOutbox}.
 *
 * <p>Saves requests to a relational database table, by default called {@code TXNO_OUTBOX}. This can
 * optionally be automatically created and upgraded by {@link JdbcPersistor}, although this
 * behaviour can be disabled if you wish.
 *
 * <p>All operations are blocking, despite returning {@link CompletableFuture}s. No attempt is made
 * to farm off the I/O to additional threads, which would be unlikely to work with JDBC {@link
 * java.sql.Connection}s. As a result, all methods should simply be called followed immediately with
 * {@link CompletableFuture#join()} to obtain the results.
 */
public class JdbcPersistor extends PersistorWrapper<Connection, JdbcTransaction<?>> {

  /**
   * Uses the default relational persistor. Shortcut for: <code>
   * JdbcPersistor.builder().dialect(dialect).build();</code>
   *
   * @param dialect The database dialect.
   * @return The persistor.
   */
  public static JdbcPersistor forDialect(Dialect dialect) {
    return builder().dialect(dialect).build();
  }

  public static JdbcPersistorBuilder builder() {
    return new JdbcPersistorBuilder();
  }

  private JdbcPersistor(SqlPersistor<Connection, JdbcTransaction<?>> delegate) {
    super(delegate);
  }

  public static final class JdbcPersistorBuilder
      extends SqlPersistor.SqlPersistorBuilder<
          Connection, JdbcTransaction<?>, JdbcPersistorBuilder> {

    private JdbcPersistorBuilder() {
      super(new JdbcSqlApi());
    }

    public JdbcPersistor build() {
      return new JdbcPersistor(super.buildGeneric());
    }
  }
}
