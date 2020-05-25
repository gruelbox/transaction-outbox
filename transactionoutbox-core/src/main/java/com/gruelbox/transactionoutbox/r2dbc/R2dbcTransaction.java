package com.gruelbox.transactionoutbox.r2dbc;

import com.gruelbox.transactionoutbox.Transaction;
import io.r2dbc.spi.Connection;

/**
 * A transaction operating on a non-blocking R2DBC {@link Connection}.
 *
 * @param <CX> The type that the client code uses to interact with the transaction.
 */
public interface R2dbcTransaction<CX> extends Transaction<Connection, CX> {}
