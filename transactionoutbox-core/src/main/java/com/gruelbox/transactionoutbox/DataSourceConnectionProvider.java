package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.Utils;
import java.sql.Connection;
import javax.sql.DataSource;
import lombok.Builder;

/**
 * A {@link ConnectionProvider} which requests connections from a {@link DataSource}. This is
 * suitable for applications using connection pools or container-provided JDBC.
 *
 * <p>Usage:
 *
 * <pre>ConnectionProvider provider = DataSourceConnectionProvider.builder()
 *   .dataSource(ds)
 *   .build()</pre>
 */
@Builder
final class DataSourceConnectionProvider implements ConnectionProvider {

  private final DataSource dataSource;

  @Override
  public Connection obtainConnection() {
    return Utils.uncheckedly(dataSource::getConnection);
  }
}
