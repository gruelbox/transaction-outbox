package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransaction;

public interface BlockingInterfaceProcessor {

  void process(int foo, String bar);

  void process(int foo, String bar, JdbcTransaction transaction);
}
