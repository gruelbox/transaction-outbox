package com.gruelbox.transactionoutbox.jdbc;

import static com.gruelbox.transactionoutbox.Utils.uncheckedly;

import java.sql.Connection;
import javax.sql.DataSource;
import lombok.Builder;

/**
 * A {@link JdbcConnectionProvider} which requests connections from a {@link DataSource}. This is
 * suitable for applications using connection pools or container-provided JDBC.
 *
 * <p>Usage:
 *
 * <pre>ConnectionProvider provider = DataSourceConnectionProvider.builder()
 *   .dataSource(ds)
 *   .build()</pre>
 */
@Builder
final class DataSourceJdbcConnectionProvider implements JdbcConnectionProvider {

  private final DataSource dataSource;

  @Override
  public Connection obtainConnection() {
    return uncheckedly(dataSource::getConnection);
  }
}
