package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.spi.TransactionManagerSupport;
import java.lang.reflect.Method;

/**
 * @deprecated Implement {@link com.gruelbox.transactionoutbox.spi.BaseTransactionManager} using
 *     {@link java.sql.Connection} as the connection type and implementing {@link
 *     #extractTransaction(Method, Object[])} and {@link #injectTransaction(Invocation,
 *     Transaction)} as below.
 */
@Beta
@Deprecated
public interface ParameterContextTransactionManager<CX> extends TransactionManager {

  Transaction transactionFromContext(CX context);

  Class<CX> contextType();

  @Override
  default TransactionalInvocation extractTransaction(Method method, Object[] args) {
    return TransactionManagerSupport.extractTransactionFromInvocation(
        method, args, contextType(), this::transactionFromContext);
  }

  @Override
  default Invocation injectTransaction(Invocation invocation, Transaction transaction) {
    return TransactionManagerSupport.injectTransactionIntoInvocation(
        invocation, contextType(), transaction);
  }
}
