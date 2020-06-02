package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.ParameterContextTransactionManager;
import java.sql.Connection;
import org.jooq.Configuration;
import org.jooq.DSLContext;

/**
 * jOOQ transaction manager which uses explicitly-passed transaction context. Suitable for use with
 * {@link org.jooq.impl.DefaultTransactionProvider}. Relies on {@link JooqTransactionListener} being
 * connected to the {@link DSLContext}.
 */
public interface DefaultJooqTransactionManager
    extends ParameterContextTransactionManager<Connection, Configuration, JooqTransaction>,
        JooqTransactionManager {}
