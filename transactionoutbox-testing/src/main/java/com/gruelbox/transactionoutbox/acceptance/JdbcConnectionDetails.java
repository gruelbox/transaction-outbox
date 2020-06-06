package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.sql.Dialect;
import lombok.Builder;
import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent = true)
@Builder
public class JdbcConnectionDetails {
  String driverClassName;
  String url;
  String user;
  String password;
  Dialect dialect;
}
