package com.gruelbox.transactionoutbox;

import java.sql.Connection;

/**
 * Source for JDBC connections to be provided to a {@link TransactionManager}. It is not required
 * for a {@link TransactionManager} to use {@link ConnectionProvider}, and when integrating with
 * existing applications with transaction management, it is indeed unlikely to do so.
 */
@SuppressWarnings("WeakerAccess")
public interface ConnectionProvider {

    /**
     * Requests a new connection, or an available connection from a pool. The caller is responsible
     * for calling {@link Connection#close()}.
     *
     * @return The connection.
     */
    Connection obtainConnection();
}
