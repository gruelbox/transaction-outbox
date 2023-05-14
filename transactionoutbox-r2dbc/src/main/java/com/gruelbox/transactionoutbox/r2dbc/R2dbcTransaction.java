package com.gruelbox.transactionoutbox.r2dbc;

import com.gruelbox.transactionoutbox.Beta;
import com.gruelbox.transactionoutbox.spi.BaseTransaction;
import io.r2dbc.spi.Connection;

/** A transaction operating on a non-blocking R2DBC {@link Connection}. */
@Beta
public interface R2dbcTransaction extends BaseTransaction<Connection> {}
