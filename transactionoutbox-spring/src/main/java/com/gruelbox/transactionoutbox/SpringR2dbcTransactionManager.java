package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransactionManager;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcRawTransaction;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcTransaction;
import com.gruelbox.transactionoutbox.r2dbc.R2dbcTransactionManager;
import com.gruelbox.transactionoutbox.spi.InitializationEventBus;
import com.gruelbox.transactionoutbox.spi.InitializationEventPublisher;
import com.gruelbox.transactionoutbox.spi.SerializableTypeRequired;
import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalSupplier;
import com.gruelbox.transactionoutbox.spi.ThrowingTransactionalWork;
import io.r2dbc.spi.Connection;

import javax.swing.*;

/** Transaction manager which uses spring-tx and Hibernate. */
public interface SpringR2dbcTransactionManager extends R2dbcTransactionManager<SpringR2dbcTransaction>, InitializationEventPublisher {

}
