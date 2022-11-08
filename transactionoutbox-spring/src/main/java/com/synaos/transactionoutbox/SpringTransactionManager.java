package com.synaos.transactionoutbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * Transaction manager which uses spring-tx and Hibernate.
 */
@Beta
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(prefix = "spring.datasource", name = "url")
@EnableScheduling
public class SpringTransactionManager implements ThreadLocalContextTransactionManager {

    private final SpringTransaction transactionInstance = new SpringTransaction();
    private final DataSource dataSource;

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void inTransaction(Runnable runnable) {
        Utils.uncheck(() -> inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromRunnable(runnable)));
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void inTransaction(TransactionalWork work) {
        Utils.uncheck(() -> inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromWork(work)));
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public <T> T inTransactionReturns(TransactionalSupplier<T> supplier) {
        return Utils.uncheckedly(
                () -> inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromSupplier(supplier)));
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public <E extends Exception> void inTransactionThrows(ThrowingTransactionalWork<E> work)
            throws E {
        inTransactionReturnsThrows(ThrowingTransactionalSupplier.fromWork(work));
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public <T, E extends Exception> T inTransactionReturnsThrows(
            ThrowingTransactionalSupplier<T, E> work) throws E {
        return work.doWork(transactionInstance);
    }

    @Override
    public <T, E extends Exception> T requireTransactionReturns(
            ThrowingTransactionalSupplier<T, E> work) throws E, NoTransactionActiveException {

        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new NoTransactionActiveException();
        }

        return work.doWork(transactionInstance);
    }

    private final class SpringTransaction implements Transaction {

        @Override
        public Connection connection() {
            return DataSourceUtils.getConnection(dataSource);
        }

        @Override
        public PreparedStatement prepareBatchStatement(String sql) {
            BatchCountingStatement preparedStatement =
                    Utils.uncheckedly(
                            () -> BatchCountingStatementHandler.countBatches(connection().prepareStatement(sql)));
            TransactionSynchronizationManager.registerSynchronization(
                    new TransactionSynchronization() {
                        @Override
                        public void beforeCommit(boolean readOnly) {
                            log.info("Flushing batches");
                            if (preparedStatement.getBatchCount() != 0) {
                                Utils.uncheck(preparedStatement::executeBatch);
                            }
                        }

                        @Override
                        public void afterCompletion(int status) {
                            Utils.safelyClose(preparedStatement);
                        }
                    });
            return preparedStatement;
        }

        @Override
        public void addPostCommitHook(Runnable runnable) {
            TransactionSynchronizationManager.registerSynchronization(
                    new TransactionSynchronization() {
                        @Override
                        public void afterCommit() {
                            runnable.run();
                        }
                    });
        }
    }

    private interface BatchCountingStatement extends PreparedStatement {

        int getBatchCount();
    }

    private static final class BatchCountingStatementHandler implements InvocationHandler {

        private final PreparedStatement delegate;
        private int count = 0;

        private BatchCountingStatementHandler(PreparedStatement delegate) {
            this.delegate = delegate;
        }

        static BatchCountingStatement countBatches(PreparedStatement delegate) {
            return (BatchCountingStatement)
                    Proxy.newProxyInstance(
                            BatchCountingStatementHandler.class.getClassLoader(),
                            new Class[]{BatchCountingStatement.class},
                            new BatchCountingStatementHandler(delegate));
        }

        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if ("getBatchCount".equals(method.getName())) {
                return count;
            }
            try {
                return method.invoke(delegate, args);
            } finally {
                if ("addBatch".equals(method.getName())) {
                    ++count;
                }
            }
        }
    }
}
