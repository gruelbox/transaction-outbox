package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.sql.SqlMigration;
import com.gruelbox.transactionoutbox.sql.SqlResultRow;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;

@Deprecated
@AllArgsConstructor
class DialectWrapper extends Dialect {

  private final com.gruelbox.transactionoutbox.sql.Dialect delegate;

  @Override
  public Stream<SqlMigration> migrations(String tableName) {
    return delegate.migrations(tableName);
  }

  @Override
  public boolean isSupportsSkipLock() {
    return delegate.isSupportsSkipLock();
  }

  @Override
  public String getIntegerCastType() {
    return delegate.getIntegerCastType();
  }

  @Override
  public String getQueryTimeoutSetup() {
    return delegate.getQueryTimeoutSetup();
  }

  @Override
  public String getDeleteExpired() {
    return delegate.getDeleteExpired();
  }

  @Override
  public String mapStatementToNative(String sql) {
    return delegate.mapStatementToNative(sql);
  }

  @Override
  public SqlResultRow mapResultFromNative(SqlResultRow row) {
    return delegate.mapResultFromNative(row);
  }
}
