package com.gruelbox.transactionoutbox;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.event.Level;

import java.util.concurrent.*;
import java.util.function.Consumer;

/**
 * Schedules background work using a local {@link Executor} implementation. Note that the {@link
 * Runnable}s submitted to this will not be {@link java.io.Serializable} so will not be suitable for
 * remoting. Remote submission of work is not yet supported.
 *
 * <p>Note that there are some important aspects that should be considered in the configuration of
 * this executor:
 *
 * <ul>
 *   <li>Should use a BOUNDED blocking queue implementation such as {@link ArrayBlockingQueue},
 *       otherwise under high volume, the queue may get so large it causes out-of-memory errors.
 *   <li>Should use a {@link java.util.concurrent.RejectedExecutionHandler} which either throws
 *       (such as {@link java.util.concurrent.ThreadPoolExecutor.AbortPolicy}), silently fails (such
 *       as {@link java.util.concurrent.ThreadPoolExecutor.DiscardPolicy}) or blocks the calling
 *       thread until a thread is available. It should <strong>not</strong> execute the work in the
 *       calling thread (e.g. {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy},
 *       since this could result in unpredictable effects with tasks assuming they will be run in a
 *       different thread context corrupting thread state. Generally, throwing or silently failing
 *       are preferred since this allows the database to absorb all backpressure, but if you have a
 *       strong reason to choose a blocking policy to enforce upstream backpressure, be aware that
 *       {@link TransactionOutbox#flush()} can potentially block for a long period of time too, so
 *       design any background processing which calls it accordingly (e.g. avoid calling from a
 *       timed scheduled job; perhaps instead simply loop it).
 *   <li>The queue can afford to be quite large in most realistic production deployments, and it is
 *       advised that it be so (10000+).
 * </ul>
 */
@Slf4j
@Builder
public class OrderedSubmitter implements Submitter, Validatable {

    /**
     * @param executor The executor to use.
     */
    @SuppressWarnings("JavaDoc")
    private final Executor executor;

    /**
     * TODO add javadoc
     */
    private final ConcurrentHashMap<String, CompletableFuture<Void>> queuedOrderedTasks = new ConcurrentHashMap<>();

    /**
     * @param logLevelWorkQueueSaturation The log level to use when work submission hits the executor
     * queue limit. This usually indicates saturation and may be of greater interest than the
     * default {@code DEBUG} level.
     */
    @SuppressWarnings("JavaDoc")
    @Builder.Default
    private final Level logLevelWorkQueueSaturation = Level.DEBUG;

    @Override
    public void submit(TransactionOutboxEntry entry, Consumer<TransactionOutboxEntry> localExecutor) {
        try {
            if (entry.getGroupId() == null) {
                executor.execute(() -> localExecutor.accept(entry));
                log.info("Submitted {} for immediate processing", entry.description());
                return;
            }

            queuedOrderedTasks.compute(entry.getGroupId(), (groupId, future) -> {
                CompletableFuture<Void> newFuture = future == null
                        ? CompletableFuture.runAsync(() -> localExecutor.accept(entry), executor)
                        : future.thenRunAsync(() -> localExecutor.accept(entry), executor);
                newFuture.thenRun(() -> cleanUpQueue(entry.getGroupId()));
                return newFuture;
            });
            log.info("Submitted {} for ordered processing", entry.description());

        } catch (RejectedExecutionException e) {
            Utils.logAtLevel(
                    log,
                    logLevelWorkQueueSaturation,
                    "Queued {} for processing when executor is available",
                    entry.description());
        } catch (Exception e) {
            log.warn(
                    "Failed to submit {} for execution. It will be re-attempted later.",
                    entry.description(),
                    e);
        }
    }

    private void cleanUpQueue(String groupId) {
        queuedOrderedTasks.entrySet().removeIf(entry -> entry.getKey().equals(groupId) && entry.getValue().isDone());
    }

    @Override
    public void validate(Validator validator) {
        validator.notNull("executor", executor);
        validator.notNull("logLevelWorkQueueSaturation", logLevelWorkQueueSaturation);
    }
}
