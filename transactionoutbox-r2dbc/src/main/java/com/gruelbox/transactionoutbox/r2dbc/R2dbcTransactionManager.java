package com.gruelbox.transactionoutbox.r2dbc;

import com.gruelbox.transactionoutbox.spi.ParameterContextTransactionManager;
import com.gruelbox.transactionoutbox.spi.TransactionManager;
import io.r2dbc.spi.Connection;

/**
 * Specialises {@link TransactionManager} for use with R2DBC.
 *
 * @param <CX> The type that the client code uses to interact with the transaction.
 */
@SuppressWarnings("WeakerAccess")
public interface R2dbcTransactionManager<CX, TX extends R2dbcTransaction<CX>>
    extends TransactionManager<Connection, CX, TX>,
        ParameterContextTransactionManager<Connection, CX, TX> {}
