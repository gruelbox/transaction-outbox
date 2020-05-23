package com.gruelbox.transactionoutbox.sql;

import lombok.Value;

@Value
final class Migration {
  int version;
  String name;
  String sql;
}
