package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.JdbcTransaction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

class JdbcShimTransaction implements Transaction {
  private final JdbcTransaction tx;

  public JdbcShimTransaction(JdbcTransaction tx) {
    this.tx = tx;
  }

  @Override
  public <T> T context() {
    return tx.context();
  }

  @Override
  public PreparedStatement prepareBatchStatement(String sql) {
    return tx.prepareBatchStatement(sql);
  }

  @Override
  public Connection connection() {
    return tx.connection();
  }

  @Override
  public void addPostCommitHook(Supplier<CompletableFuture<Void>> hook) {
    tx.addPostCommitHook(hook);
  }

  JdbcTransaction getDelegate() {
    return tx;
  }
}
