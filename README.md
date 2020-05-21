# transaction-outbox
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transactionoutbox-core/badge.svg)](#stable-releases)
[![Javadocs](https://www.javadoc.io/badge/com.gruelbox/transactionoutbox-core.svg?color=blue)](https://www.javadoc.io/doc/com.gruelbox/transactionoutbox-core)
[![GitHub Release Date](https://img.shields.io/github/release-date/gruelbox/transaction-outbox)](https://github.com/gruelbox/transaction-outbox/releases/latest)
[![Latest snapshot](https://img.shields.io/github/v/tag/gruelbox/transaction-outbox?label=snapshot&sort=semver)](#development-snapshots)
[![GitHub last commit](https://img.shields.io/github/last-commit/gruelbox/transaction-outbox)](https://github.com/gruelbox/transaction-outbox/commits/master)
[![CD](https://github.com/gruelbox/transaction-outbox/workflows/Continous%20Delivery/badge.svg)](https://github.com/gruelbox/transaction-outbox/actions)
[![CodeFactor](https://www.codefactor.io/repository/github/gruelbox/transaction-outbox/badge)](https://www.codefactor.io/repository/github/gruelbox/transaction-outbox)

A flexible implementation of the [Transaction Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html) for Java. `TransactionOutbox` has a clean, extensible API, very few dependencies and plays nicely with a variety of database platforms, transaction management approaches and application frameworks.  Every aspect is highly configurable or overridable.  It features out-of-the-box support for **Spring DI**, **Spring Txn**, **Hibernate**, **Guice**, **MySQL 5 & 8**, **PostgreSQL 9-12** and **H2**.

## Contents
1. [Why do I need it?](#why-do-i-need-it)
1. [Installation](#installation)
   1. [Stable releases](#stable-releases)
   1. [Development snapshots](#development-snapshots)
1. [Basic Configuration](#basic-configuration)
   1. [No existing transaction manager or dependency injection](#no-existing-transaction-manager-or-dependency-injection)
   1. [Spring](#spring)
   1. [Guice](#guice)
   1. [jOOQ](#jooq)
1. [Set up the background worker](#set-up-the-background-worker)
1. [Managing the "dead letter queue"](#managing-the-dead-letter-queue)
1. [Idempotency protection](#idempotency-protection)
1. [Configuration reference](#configuration-reference)
1. [Stubbing in tests](#stubbing-in-tests)

## Why do I need it?

[This article](https://microservices.io/patterns/data/transactional-outbox.html) explains the concept in an abstract manner, but let's say we have a microservice that handles point-of-sale and need to implement a REST endpoint to record a sale. We end up with this:

### Attempt 1

```java
@POST
@Path("/sales")
@Transactional
public SaleId createWidget(Sale sale) {
  var saleId = saleRepository.save(sale);
  messageQueue.postMessage(StockReductionEvent.of(sale.item(), sale.amount()));
  messageQueue.postMessage(IncomeEvent.of(sale.value()));
  return saleId;
}
```
The `SaleRepository` handles recording the sale in the customer's account, the `StockReductionEvent` goes off to our _warehouse_ service, and the `IncomeEvent` goes to our financial records service (let's ignore the potential flaws in the domain modelling for now).

There's a big problem here: the `@Transactional` annotation is a lie (no, [really](https://lmgtfy.com/?q=dont+use+distributed+transactions)). It only really wraps the `SaleRepository` call, but not the two event postings. This means that we could end up sending the two events and fail to actually commit the sale. Our system is now inconsistent.

### Attempt 2 - Use Idempotency

We could make whole method [idempotent](http://restcookbook.com/HTTP%20Methods/idempotency/) and re-write it to work a bit more like this:

```java
@PUT
@Path("/sales/{id}")
public void createWidget(@PathParam("id") SaleId saleId, Sale sale) {
  saleRepository.saveInNewTransaction(saleId, sale);
  messageQueue.postMessage(StockReductionEvent.of(saleId, sale.item(), sale.amount()));
  messageQueue.postMessage(IncomeEvent.of(saleId, sale.value()));
}
```
This is better. As long as the caller keeps calling the method until they get a success, we can keep re-saving and re-sending the messages without any risk of duplicating work.  This works regardless of the order of the calls (and in any case, there may be good reasons of referential integrity to fix the order).

The problem is that _they might stop trying_, and if they do, we could end up with only part of this transaction completed. If this is a public API, we can't force clients to use it correctly.

We also still have another problem: external calls are inherently more vulnerable to downtime and performance degredation.  We could find our service rendered unresponsive or failing if they are unavailable. Ideally, we would like to "buffer" these external calls within our service safely until our downstream dependencies are available.

### Attempt 3 - Transaction Outbox

Idempotency is a good thing, so let's stick with the `PUT`. Here is the same example, using Transaction Outbox:

```java
@PUT
@Path("/sales/{id}")
@Transactional
public void createWidget(@PathParam("id") SaleId saleId, Sale sale) {
  saleRepository.save(saleId, sale);
  MessageQueue proxy = transactionOutbox.schedule(MessageQueue.class);
  proxy.postMessage(StockReductionEvent.of(saleId, sale.item(), sale.amount()));
  proxy.postMessage(IncomeEvent.of(saleId, sale.value()));
}
```
Here's what happens:

 - [`TransactionOutbox`](https://www.javadoc.io/static/com.gruelbox/transactionoutbox-core/0.1.57/com/gruelbox/transactionoutbox/TransactionOutbox.html) creates a proxy of `MessageQueue`. Any method calls on the proxy are serialized and written to a database table _in the same transaction_ as the `SaleRepository` call. The call returns immediately rather than actually invoking the real method.
 - If the transaction rolls back, so do the serialized requests.
 - Immediately after the transaction is successfully committed, another thread will attempt to make the _real_ call to `MessageQueue` asynchronously.
 - If that call fails, or the application dies before the call is attempted, a [background "mop-up" thread](#set-up-the-background-worker) will re-attempt the call a configurable number of times, with configurable time between each, before [blacklisting](#managing-the-dead-letter-queue) the request and firing and event for it to be investigated (similar to a [dead letter queue](https://en.wikipedia.org/wiki/Dead_letter_queue)).
 - Blacklisted requests can be easily [whitelisted](#managing-the-dead-letter-queue) again once the underlying issue is resolved.

Our service is now resilient and explicitly eventually consistent, as long as all three elements (`SaleRepository` and the downstream event handlers) are idempotent, since those messages will be attempted repeatedly until confirmed successful, which means they could occur multiple times.

If you find yourself wondering _why bother with the queues now_? You're quite right. As we now have outgoing buffers, we already have most of the benefits of middleware (at least for some use cases). We could replace the calls to a message queue with direct queues to the other services' load balancers and switch to a peer-to-peer architecture, if we so choose.

> Note that for the above example to work, `StockReductionEvent` and `IncomeEvent` need to be whitelisted for serialization. See [Configuration reference](#configuration-reference).

## Installation

> Requires at least Java 11. **Stuck on an earlier JDK? [Speak up](https://github.com/gruelbox/transaction-outbox/issues/new/choose)**. If there's any interest in downgrading, it won't be particularly hard to strip out the Java 9/10/11 features like `var`.

### Stable releases

The latest stable release is available from Maven Central. The latest version is: [![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transactionoutbox-core/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transactionoutbox-core).

#### Maven
```xml
<dependency>
  <groupId>com.gruelbox</groupId>
  <artifactId>transactionoutbox-core</artifactId>
  <version>${transactionoutbox.version}</version>
</dependency>
```
#### Gradle
```groovy
implementation 'com.gruelbox:transactionoutbox-core:$transactionOutboxVersion'
```
### Development snapshots

Maven Central is updated regularly. Alternatively, if you want to stay at the bleeding edge, you can use continuously-delivered releases from [Github Package Repository](https://github.com/gruelbox/transaction-outbox/packages). These can be used from production builds since they will never be deleted.

#### Maven
```xml
<repositories>
  <repository>
    <id>github-transaction-outbox</id>
    <name>Gruelbox Github Repository</name>
    <url>https://maven.pkg.github.com/gruelbox/transaction-outbox</url>
  </repository>
</repositories>
```
You will need to authenticate with Github to use Github Package Repository. Create a personal access token in [your GitHub settings](https://github.com/settings/tokens). It only needs **read:package** permissions. Then add something like the following in your Maven `settings.xml`:
```xml
<servers>
    <server>
        <id>github-transaction-outbox</id>
        <username>${env.GITHUB_USERNAME}</username>
        <password>${env.GITHUB_TOKEN}</password>
    </server>
</servers>
```
The above example uses environment variables, allowing you to keep the credentials out of source control, but you can hard-code them if you know what you're doing.

#### Gradle
```groovy
repositories {
    maven {
        name = "github-transaction-outbox"
        url = uri("https://maven.pkg.github.com/gruelbox/transaction-outboxY")
        credentials {
            username = $githubUserName
            password = $githubToken
        }
    }
}
```

## Basic Configuration

An application needs a single, shared instance of [`TransactionOutbox`](https://www.javadoc.io/static/com.gruelbox/transactionoutbox-core/0.1.57/com/gruelbox/transactionoutbox/TransactionOutbox.html), which is configured using a builder on construction. This takes some time to get right, particularly if you already have a transaction management solution in your application.

### No existing transaction manager or dependency injection
If you have no existing transaction management, connection pooling or dependency injection, here's a quick way to get started:
```java
// Use an in-memory H2 database
TransactionManager transactionManager = TransactionManager.fromConnectionDetails(
    "org.h2.Driver", "jdbc:h2:mem:test;MV_STORE=TRUE", "test", "test"));

// Create the outbox
TransactionOutbox outbox = TransactionOutbox.builder()
  .transactionManager(transactionManager)
  .persistor(Persistor.forDialect(Dialect.H2))
  .build();

// Start a transaction
transactionManager.inTransaction(tx -> {
  // Save some stuff
  tx.connection().createStatement().execute("INSERT INTO...");
  // Create an outbox request
  outbox.schedule(MyClass.class).myMethod("Foo", "Bar"));
});
```
Alternatively, you could create the [`TransactionManager`](https://www.javadoc.io/doc/com.gruelbox/transactionoutbox-core/latest/com/gruelbox/transactionoutbox/TransactionManager.html) from a [`DataSource`](https://www.javadoc.io/doc/com.gruelbox/transactionoutbox-core/latest/com/gruelbox/transactionoutbox/TransactionManager.html), allowing you to use a connection pooling `DataSource` such as Hikari:

```java
TransactionManager transactionManager = TransactionManager.fromDataSource(dataSource);
```

In this default configuration, `MyClass` must have a default constructor so the "real" implementation can be constructed at the point the method is actually invoked (which might be on another day on another instance of the application). However, you can avoid this requirement by providing an [`Instantiator`](https://www.javadoc.io/doc/com.gruelbox/transactionoutbox-core/latest/com/gruelbox/transactionoutbox/Instantiator.html) on every instance of your application that knows how to create the objects:
```java
TransactionOutbox outbox = TransactionOutbox.builder()
  .instantiator(Instantiator.using(clazz -> createInstanceOf(clazz)))
  .build();
```

### Spring

See [transaction-outbox-spring](transactionoutbox-spring/README.md), which integrates Spring's DI and/or transaction management with `TransactionOutbox`.

### Guice

See [transaction-outbox-guice](transactionoutbox-guice/README.md), which integrates Guice DI  `TransactionOutbox`.

### jOOQ

See [transaction-outbox-jooq](transactionoutbox-jooq/README.md), which integrates jOOQ transaction management with `TransactionOutbox`.

## Set up the background worker

At the moment, if any work fails first time, it won't be retried.  All we need to add is a background task that repeatedly calls [`TransactionOutbox.flush()`](https://www.javadoc.io/doc/com.gruelbox/transactionoutbox-core/latest/com/gruelbox/transactionoutbox/TransactionOutbox.html), e.g:
```java
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(outbox::flush, 2, 2, TimeUnit.MINUTES);
```
Or with RxJava:
```java
Observable.interval(2, MINUTES).subscribe(i -> outbox.flush());
```
Wire this into your app in whichever way works best. Don't worry about it running on multiple instances simultaneously. It's designed to handle concurrent use, and indeed it can be a benefit; spreading high workloads across instances without any need for more complex high-availability configuration (that said, if you want to distribute work across a cluster at point of submission, this is also supported).

## Managing the "dead letter queue"
Work might be retried too many times and get blacklisted. You should set up an alert to allow you to manage this when it occurs, resolve the issue and un-blacklist the work, since the work not being complete will usually be a sign that your system is out of sync in some way.
```java
TransactionOutbox.builder()
    ...
    .listener(new TransactionOutboxListener() {
        @Override
        public void blacklisted(TransactionOutboxEntry entry, Throwable cause) {
           // Spring example
           applicationEventPublisher.publishEvent(new TransactionOutboxBlacklistEvent(entry.getId(), cause);
        }
    })
    .build();
```
To mark the work for reprocessing, just use [`TransactionOutbox.whitelist()`](https://www.javadoc.io/doc/com.gruelbox/transactionoutbox-core/latest/com/gruelbox/transactionoutbox/TransactionOutbox.html). Its failure count will be marked back down to zero and it will get reprocessed on the next call to `flush()`:
```
transactionOutboxEntry.whitelist(entryId);
```
Or if using a `TransactionManager` that relies on explicit context (such as a non-thread local) [`JooqTransactionManager`](https://www.javadoc.io/doc/com.gruelbox/transactionoutbox-jooq/latest/com/gruelbox/transactionoutbox/JooqTransactionManager.html):
```
transactionOutboxEntry.whitelist(entryId, context);
```

A good approach here is to use the [`TransactionOutboxListener`](https://www.javadoc.io/doc/com.gruelbox/transactionoutbox-core/latest/com/gruelbox/transactionoutbox/TransactionOutboxListener.html) callback to post an [interactive Slack message](https://api.slack.com/legacy/interactive-messages) - this can operate as both the alert and the "button" allowing a support engineer to submit the work for reprocessing.

## Idempotency protection

A common use case for `TransactionOutbox` is to receive an incoming request (such as a message from a message queue), acknowledge it immediately and process it asynchronously, for example:

```java
public class FooEventHandler implements SQSEventHandler<ThingHappenedEvent> {

  @Inject private TransactionOutbox outbox;

  public void handle(ThingHappenedEvent event) {
    outbox.schedule(FooService.class).handleEvent(event.getThingId());
  }
}
```
However, incoming transports, whether they be message queues or APIs, usually need to rely on idempotency in message handlers (for the same reason that outgoing requests from outbox also rely on idempotency). This means the above code could get called twice.

As long as `FooService.handleEvent()` is idempotent itself, this is harmless, but we can't always assume this. The incoming message might be a broadcast, with no knowledge of the behaviour of handlers and therefore no way of pre-generating any new record ids the handler might need and passing them in the message.

To protect against this, `TransactionOutbox` can automatically detect duplicate requests and reject them with `AlreadyScheduledException`. Records of requests are retained up to a configurable threshold (see below).

To use this, use the call pattern:

```java
outbox.with()
  .uniqueRequestId("context-clientid")
  .schedule(Service.class)
  .process("Foo");
```

Where `context-clientid` is a globally-unique identifier derived from the incoming request.  Such ids are usually available from queue middleware as message ids, or if not you can require as part of the incoming API (possibly with a tenant prefix to ensure global uniqueness across tenants).

## Configuration reference

This example shows a number of other configuration options in action:

```java
TransactionManager transactionManager = TransactionManager.fromDataSource(dataSource);

TransactionOutbox outbox = TransactionOutbox.builder()
    // The most complex part to set up for most will be synchronizing with your existing transaction
    // management. Pre-rolled implementations are available for jOOQ and Spring (see above for more information)
    // and you can use those examples to synchronize with anything else by defining your own TransactionManager.
    // Or, if you have no formal transaction management at the moment, why not start, using transaction-outbox's
    // built-in one?
    .transactionManager(transactionManager)
    // Modify how requests are persisted to the database.
    .persistor(DefaultPersistor.builder()
        // Selecting the right SQL dialect ensures that features such as SKIP LOCKED are used correctly.
        .dialect(Dialect.POSTGRESQL_9)
        // Override the table name (defaults to "TXNO_OUTBOX")
        .tableName("transactionOutbox") 
        // Shorten the time we will wait for write locks (defaults to 2)
        .writeLockTimeoutSeconds(1)
        // Disable automatic creation and migration of the outbox table, forcing the application to manage
        // migrations itself
        .migrate(false)
        // Allow the SaleType enum and Money class to be used in arguments (see example below)
        .serializer(DefaultInvocationSerializer.builder()
            .whitelistedTypes(Set.of(SaleType.class, Money.class))
            .build())
        .build())
    // GuiceInstantiator and SpringInstantiator are great if you are using Guice or Spring DI, but what if you
    // have your own service locator? Wire it in here. Fully-custom Instantiator implementations are easy to
    // implement.
    .instantiator(Instantiator.using(myServiceLocator::createInstance))
    // Change the log level used when work cannot be submitted to a saturated queue to INFO level (the default
    // is WARN, which you should probably consider a production incident). You can also change the Executor used
    // for submitting work to a shared thread pool used by the rest of your application. Fully-custom Submitter
    // implementations are also easy to implement.
    .submitter(ExecutorSubmitter.builder()
        .executor(ForkJoinPool.commonPool())
        .logLevelWorkQueueSaturation(Level.INFO)
        .build())
    // Lower the log level when a task fails temporarily from the default WARN.
    .logLevelTemporaryFailure(Level.INFO)
    // 10 attempts at a task before blacklisting it
    .blacklistAfterAttempts(10)
    // When calling flush(), select 0.5m records at a time.
    .flushBatchSize(500_000)
    // Flush once every 15 minutes only
    .attemptFrequency(Duration.ofMinutes(15))
    // Include Slf4j's Mapped Diagnostic Context in tasks. This means that anything in the MDC when schedule()
    // is called will be recreated in the task when it runs. Very useful for tracking things like user ids and
    // request ids across invocations.
    .serializeMdc(true)
    // Sets how long we should keep records of requests with a unique request id so duplicate requests
    // can be rejected. Defaults to 7 days.
    .retentionThreshold(Duration.ofDays(1))
    // We can intercept task successes, failures and blacklistings. The most common use is to catch blacklistings
    // and raise alerts for these to be investigated. A Slack interactive message is particularly effective here
    // since it can be wired up to call whitelist() automatically.
    .listener(new TransactionOutboxListener() {

      @Override
      public void success(TransactionOutboxEntry entry) {
        eventPublisher.publish(new OutboxTaskProcessedEvent(entry.getId()));
      }

      @Override
      public void blacklisted(TransactionOutboxEntry entry, Throwable cause) {
        eventPublisher.publish(new BlacklistedOutboxTaskEvent(entry.getId()));
      }

    })
    .build();

// Usage example, using the in-built transaction manager
MDC.put("SESSIONKEY", "Foo");
try {
  transactionManager.inTransaction(tx -> {
    writeSomeChanges(tx.connection());
    outbox.schedule(getClass())
        .performRemoteCall(SaleType.SALE, Money.of(10, Currency.getInstance("USD")));
  });
} finally {
  MDC.clear();
}
```

## Stubbing in tests

`TransactionOutbox` should not be directly stubbed (e.g. using Mockito); the contract is too complex to stub out.

Instead, [stubs](https://www.javadoc.io/doc/com.gruelbox/transactionoutbox-core/latest/com/gruelbox/transactionoutbox/StubTransactionManager.html) [exist](https://www.javadoc.io/doc/com.gruelbox/transactionoutbox-core/latest/com/gruelbox/transactionoutbox/StubPersistor.html) for the various arguments to the builder, allowing you to build a `TransactionOutbox` with minimal external dependencies which can be called and verified in tests.
```java
// GIVEN

SomeService mockService = Mockito.mock(SomeService.class);

// Also see StubParameterContextTransactionManager
StubTransactionManager transactionManager = new StubThreadLocalTransactionManager();

TransactionOutbox outbox = TransactionOutbox.builder()
    .instantiator(Instantiator.using(clazz -> mockService)) // Return our mock
    .persistor(StubPersistor.builder().build()) // Doesn't save anything
    .submitter(Submitter.withExecutor(MoreExecutors.directExecutor())) // Execute all work in-line
    .clockProvider(() ->
        Clock.fixed(LocalDateTime.of(2020, 3, 1, 12, 0)
            .toInstant(ZoneOffset.UTC), ZoneOffset.UTC)) // Fix the clock (not necessary here)
    .transactionManager(transactionManager)
    .build();

// WHEN
transactionManager.inTransaction(tx -> 
   outbox.schedule(SomeService.class).doAThing(1));

// THEN 
Mockito.verify(mockService).doAThing(1);
```

Depending on the type of test, you may wish to use a real `Persistor` such as `DefaultPersistor` (if there's a real database available) or a real, multi-threaded `Submitter`. That's up to you.
