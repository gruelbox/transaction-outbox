package com.gruelbox.transactionoutbox.sql;

import com.gruelbox.transactionoutbox.Beta;

@Beta
public interface SqlResultRow {
  <T> T get(int index, Class<T> type);
}
