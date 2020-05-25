package com.gruelbox.transactionoutbox.r2dbc;

import com.gruelbox.transactionoutbox.ParameterContextTransactionManager;
import com.gruelbox.transactionoutbox.TransactionManager;
import io.r2dbc.spi.Connection;

/**
 * Specialises {@link TransactionManager} for use with R2DBC.
 *
 * @param <CX> The type that the client code uses to interact with the transaction.
 */
public interface R2dbcTransactionManager<CX, TX extends R2dbcTransaction<CX>>
    extends TransactionManager<Connection, CX, TX>,
        ParameterContextTransactionManager<Connection, CX, TX> {}
