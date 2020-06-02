package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransactionManager;
import com.gruelbox.transactionoutbox.spi.ThreadLocalContextTransactionManager;
import java.sql.Connection;

/** Transaction manager which uses spring-tx and Hibernate. */
public interface SpringTransactionManager
    extends JdbcTransactionManager<Void, SpringTransaction>,
        ThreadLocalContextTransactionManager<Connection, Void, SpringTransaction> {}
