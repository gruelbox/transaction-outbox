package com.gruelbox.transactionoutbox.r2dbc;

import com.gruelbox.transactionoutbox.spi.BaseTransactionManager;
import io.r2dbc.spi.Connection;

/**
 * Specialises {@link BaseTransactionManager} for use with R2DBC.
 *
 * @param <TX> The transaction type.
 */
@SuppressWarnings("WeakerAccess")
public interface R2dbcTransactionManager<TX extends R2dbcTransaction>
    extends BaseTransactionManager<Connection, TX> {}
