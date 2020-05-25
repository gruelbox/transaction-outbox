package com.gruelbox.transactionoutbox.r2dbc;

import com.ea.async.Async;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.InvocationSerializer;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.jdbc.JdbcPersistor;
import io.r2dbc.spi.Connection;
import java.io.StringWriter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import reactor.core.publisher.Mono;

/**
 * An R2DBC-based {@link Persistor} for {@link TransactionOutbox}, using purely the low-level R2DBC
 * API, so compatible with any R2DBC client API such as Spring. All operations are non-blocking.
 *
 * <p>Saves requests to a relational database table, by default called {@code TXNO_OUTBOX}. This can
 * optionally be automatically created and upgraded by {@link JdbcPersistor}, although this
 * behaviour can be disabled if you wish.
 *
 * <p>More significant changes can be achieved by subclassing, which is explicitly supported. If, on
 * the other hand, you want to use a completely non-relational underlying data store or do something
 * equally esoteric, you may prefer to implement {@link Persistor} from the ground up.
 */
@Builder
public class R2dbcPersistor implements Persistor<Connection, R2dbcTransaction<?>> {

  /**
   * Uses the default relational non-blocking persistor. Shortcut for: <code>
   * R2dbcPersistor.builder().dialect(dialect).build();</code>
   *
   * @param dialect The database dialect.
   * @return The persistor.
   */
  public static R2dbcPersistor forDialect(Dialect dialect) {
    return R2dbcPersistor.builder().dialect(dialect).build();
  }

  /**
   * @param writeLockTimeoutSeconds How many seconds to wait before timing out on obtaining a write
   *     lock. There's no point making this long; it's always better to just back off as quickly as
   *     possible and try another record. Generally these lock timeouts only kick in if {@link
   *     Dialect#isSupportsSkipLock()} is false.
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  @NotNull
  private final int writeLockTimeoutSeconds = 2;

  /** @param dialect The database dialect to use. Required. */
  @SuppressWarnings("JavaDoc")
  @NotNull
  private final Dialect dialect;

  /** @param tableName The database table name. The default is {@code TXNO_OUTBOX}. */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  @NotNull
  private final String tableName = "TXNO_OUTBOX";

  /**
   * @param migrate Set to false to disable automatic database migrations. This may be preferred if
   *     the default migration behaviour interferes with your existing toolset, and you prefer to
   *     manage the migrations explicitly (e.g. using FlyWay or Liquibase), or your do not give the
   *     application DDL permissions at runtime.
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  @NotNull
  private final boolean migrate = true;

  /**
   * @param serializer The serializer to use for {@link Invocation}s. See {@link
   *     InvocationSerializer} for more information. Defaults to {@link
   *     InvocationSerializer#createDefaultJsonSerializer()} with no whitelisted classes..
   */
  @SuppressWarnings("JavaDoc")
  @Builder.Default
  private final InvocationSerializer serializer =
      InvocationSerializer.createDefaultJsonSerializer();

  @Override
  public CompletableFuture<Void> migrate(TransactionManager transactionManager) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Void> save(R2dbcTransaction<?> tx, TransactionOutboxEntry entry) {
    var writer = new StringWriter();
    serializer.serializeInvocation(entry.getInvocation(), writer);
    var statement =
        tx.connection()
            .createStatement(
                "INSERT INTO "
                    + tableName
                    + " (id, uniqueRequestId, invocation, nextAttemptTime, attempts, blacklisted, processed, version) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(0, entry.getId())
            .bind(2, writer.toString())
            .bind(3, LocalDateTime.ofInstant(entry.getNextAttemptTime(), ZoneId.of("Z")))
            .bind(4, entry.getAttempts())
            .bind(5, entry.isBlacklisted())
            .bind(6, entry.isProcessed())
            .bind(7, entry.getVersion());
    if (entry.getUniqueRequestId() == null) {
      statement = statement.bindNull(1, String.class);
    } else {
      statement = statement.bind(1, entry.getUniqueRequestId());
    }
    Async.await(Mono.from(statement.execute()).toFuture());
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> delete(R2dbcTransaction<?> tx, TransactionOutboxEntry entry) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Void> update(R2dbcTransaction<?> tx, TransactionOutboxEntry entry) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Boolean> lock(R2dbcTransaction<?> tx, TransactionOutboxEntry entry) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Boolean> whitelist(R2dbcTransaction<?> tx, String entryId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<List<TransactionOutboxEntry>> selectBatch(
      R2dbcTransaction<?> tx, int batchSize, Instant now) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Integer> deleteProcessedAndExpired(
      R2dbcTransaction<?> tx, int batchSize, Instant now) {
    throw new UnsupportedOperationException();
  }
}
