package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransaction;

@SuppressWarnings("WeakerAccess")
public interface SpringTransaction extends JdbcTransaction {}
