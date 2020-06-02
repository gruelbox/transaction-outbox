package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.ThreadLocalContextTransactionManager;
import java.sql.Connection;
import org.jooq.Configuration;
import org.jooq.DSLContext;

/**
 * jOOQ transaction manager which uses thread-local context. Best used with {@link
 * org.jooq.impl.ThreadLocalTransactionProvider}. Relies on a {@link JooqTransactionListener} being
 * attached to the {@link DSLContext}.
 */
public interface ThreadLocalJooqTransactionManager
    extends ThreadLocalContextTransactionManager<Connection, Configuration, JooqTransaction>,
        JooqTransactionManager {}
