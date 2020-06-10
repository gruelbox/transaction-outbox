package com.gruelbox.transactionoutbox.r2dbc;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.jdbc.JdbcPersistor;
import com.gruelbox.transactionoutbox.PersistorWrapper;
import com.gruelbox.transactionoutbox.sql.Dialect;
import com.gruelbox.transactionoutbox.sql.SqlPersistor;
import io.r2dbc.spi.Connection;
import lombok.extern.slf4j.Slf4j;

/**
 * An R2DBC-based {@link Persistor} for {@link TransactionOutbox}, using purely the low-level R2DBC
 * API, so compatible with any R2DBC client API such as Spring. All operations are non-blocking.
 *
 * <p>Saves requests to a relational database table, by default called {@code TXNO_OUTBOX}. This can
 * optionally be automatically created and upgraded by {@link JdbcPersistor}, although this
 * behaviour can be disabled if you wish.
 */
@Slf4j
@Beta
public class R2dbcPersistor extends PersistorWrapper<Connection, R2dbcTransaction> {

  public static R2dbcPersistor forDialect(Dialect dialect) {
    return builder().dialect(dialect).build();
  }

  public static R2dbcPersistorBuilder builder() {
    return new R2dbcPersistorBuilder();
  }

  private R2dbcPersistor(SqlPersistor<Connection, R2dbcTransaction> delegate) {
    super(delegate);
  }

  public static final class R2dbcPersistorBuilder
      extends SqlPersistor.SqlPersistorBuilder<
          Connection, R2dbcTransaction, R2dbcPersistorBuilder> {
    private R2dbcPersistorBuilder() {
      super(new R2dbcSqlApi());
    }

    public R2dbcPersistor build() {
      return new R2dbcPersistor(super.buildGeneric());
    }
  }
}
