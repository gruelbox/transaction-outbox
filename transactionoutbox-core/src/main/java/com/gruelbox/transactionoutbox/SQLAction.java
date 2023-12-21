package com.gruelbox.transactionoutbox;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
interface SQLAction {
  void doAction(Connection connection) throws SQLException;
}
