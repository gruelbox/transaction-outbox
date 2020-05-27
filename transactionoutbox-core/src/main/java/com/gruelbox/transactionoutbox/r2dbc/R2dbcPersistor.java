package com.gruelbox.transactionoutbox.r2dbc;

import static com.ea.async.Async.await;
import static com.gruelbox.transactionoutbox.Utils.toRunningFuture;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import com.gruelbox.transactionoutbox.AlreadyScheduledException;
import com.gruelbox.transactionoutbox.Dialect;
import com.gruelbox.transactionoutbox.Invocation;
import com.gruelbox.transactionoutbox.InvocationSerializer;
import com.gruelbox.transactionoutbox.OptimisticLockException;
import com.gruelbox.transactionoutbox.Persistor;
import com.gruelbox.transactionoutbox.TransactionManager;
import com.gruelbox.transactionoutbox.TransactionOutbox;
import com.gruelbox.transactionoutbox.TransactionOutboxEntry;
import com.gruelbox.transactionoutbox.jdbc.JdbcPersistor;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcDataIntegrityViolationException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.Statement;
import java.io.StringReader;
import java.io.StringWriter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import javax.validation.constraints.NotNull;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
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
@Slf4j
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
  public CompletableFuture<Void> migrate(
      TransactionManager<Connection, ?, ? extends R2dbcTransaction<?>> transactionManager) {
    return R2dbcMigrationManager.migrate(transactionManager);
  }

  @Override
  public CompletableFuture<Void> save(R2dbcTransaction<?> tx, TransactionOutboxEntry entry) {
    var writer = new StringWriter();
    serializer.serializeInvocation(entry.getInvocation(), writer);
    try {
      await(
          execute(
              tx,
              "INSERT INTO "
                  + tableName
                  + " (id, uniqueRequestId, invocation, nextAttemptTime, attempts, blacklisted, processed, version) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
              entry.getId(),
              entry.getUniqueRequestId(),
              writer.toString(),
              LocalDateTime.ofInstant(entry.getNextAttemptTime(), ZoneId.of("Z")),
              entry.getAttempts(),
              entry.isBlacklisted(),
              entry.isProcessed(),
              entry.getVersion()));
      log.debug("Inserted {} immediately", entry.description());
      return completedFuture(null);
    } catch (CompletionException e) {
      if (e.getCause() instanceof R2dbcDataIntegrityViolationException) {
        return failedFuture(
            new AlreadyScheduledException("Request " + entry.description() + " already exists", e));
      }
      return failedFuture(e.getCause());
    }
  }

  @Override
  public CompletableFuture<Void> delete(R2dbcTransaction<?> tx, TransactionOutboxEntry entry) {
    Result result =
        await(
            execute(
                tx,
                "DELETE FROM " + tableName + " WHERE id = ? and version = ?",
                entry.getId(),
                entry.getVersion()));
    Integer rowsUpdated = await(toRunningFuture(result.getRowsUpdated()));
    if (rowsUpdated != 1) {
      return failedFuture(new OptimisticLockException());
    }
    log.debug("Deleted {}", entry.description());
    return completedFuture(null);
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
    String forUpdate = dialect.isSupportsSkipLock() ? " FOR UPDATE SKIP LOCKED" : "";
    String sql =
        "SELECT id, uniqueRequestId, invocation, nextAttemptTime, attempts, blacklisted, processed, version FROM "
            + tableName
            + " WHERE nextAttemptTime < ? AND blacklisted = false AND processed = false LIMIT ?"
            + forUpdate;
    return executeQuery(tx, sql, LocalDateTime.now(ZoneOffset.UTC), batchSize)
        .map(this::map)
        .collect(Collectors.toList())
        .toFuture();
  }

  @Override
  public CompletableFuture<Integer> deleteProcessedAndExpired(
      R2dbcTransaction<?> tx, int batchSize, Instant now) {
    throw new UnsupportedOperationException();
  }

  private CompletableFuture<? extends Result> execute(
      R2dbcTransaction<?> tx, String sql, Object... args) {
    Statement statement = buildStatement(tx, sql, args);
    return toRunningFuture(statement.execute());
  }

  private Flux<Row> executeQuery(R2dbcTransaction<?> tx, String sql, Object... args) {
    return Mono.from(buildStatement(tx, sql, args).execute())
        .flatMapMany(result -> Flux.from(result.map((row, meta) -> row)));
  }

  private Statement buildStatement(R2dbcTransaction<?> tx, String sql, Object[] args) {
    var statement = tx.connection().createStatement(sql);
    for (int i = 0; i < args.length; i++) {
      Object arg = args[i];
      if (arg == null) {
        statement = statement.bindNull(i, String.class); // Lazy, but does what we need here
      } else {
        statement = statement.bind(i, arg);
      }
    }
    return statement;
  }

  @SuppressWarnings("ConstantConditions")
  private TransactionOutboxEntry map(Row rs) {
    return TransactionOutboxEntry.builder()
        .id(rs.get("id", String.class))
        .uniqueRequestId(rs.get("uniqueRequestId", String.class))
        .invocation(
            serializer.deserializeInvocation(new StringReader(rs.get("invocation", String.class))))
        .nextAttemptTime(rs.get("nextAttemptTime", LocalDateTime.class).toInstant(ZoneOffset.UTC))
        .attempts(rs.get("attempts", Integer.class))
        .blacklisted(rs.get("blacklisted", Boolean.class))
        .processed(rs.get("processed", Boolean.class))
        .version(rs.get("version", Integer.class))
        .build();
  }

  // For testing. Assumed low volume.
  CompletableFuture<Void> clear(R2dbcTransaction<?> tx) {
    return toRunningFuture(tx.connection().createStatement("DELETE FROM " + tableName).execute())
        .thenApply(__ -> null);
  }
}
