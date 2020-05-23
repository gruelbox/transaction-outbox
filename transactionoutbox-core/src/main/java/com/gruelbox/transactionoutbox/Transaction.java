package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransaction;

/**
 * Represents a transaction in JDBC-land.
 *
 * @deprecated Use {@link com.gruelbox.transactionoutbox.jdbc.JdbcTransaction}.
 */
@Deprecated
public interface Transaction extends JdbcTransaction {}
