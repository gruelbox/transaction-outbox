package com.gruelbox.transactionoutbox.spi;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;

final class BatchCounter implements InvocationHandler {

  private final PreparedStatement delegate;
  private int count = 0;

  BatchCounter(PreparedStatement delegate) {
    this.delegate = delegate;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    if ("getBatchCount".equals(method.getName())) {
      return count;
    }
    try {
      return method.invoke(delegate, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    } finally {
      if ("addBatch".equals(method.getName())) {
        ++count;
      } else if ("executeBatch".equals(method.getName())) {
        count = 0;
      }
    }
  }
}
