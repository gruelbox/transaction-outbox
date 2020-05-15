# transaction-outbox
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transaction-outbox/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transaction-outbox)
[![Javadocs](https://www.javadoc.io/badge/com.gruelbox/transaction-outbox.svg?color=blue)](https://www.javadoc.io/doc/com.gruelbox/transaction-outbox)
[![CD](https://github.com/gruelbox/transaction-outbox/workflows/Continous%20Delivery/badge.svg)](https://github.com/gruelbox/transaction-outbox/actions)
[![CodeFactor](https://www.codefactor.io/repository/github/gruelbox/transaction-outbox/badge)](https://www.codefactor.io/repository/github/gruelbox/transaction-outbox)

A flexible implementation of the [Transaction Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html) for Java. Can replace or augment message queues such as RabbitMQ or ApacheMQ, while additionally allowing local work and remote requests to be committed in the same database transaction, guaranteeing eventual consistency*.

Transaction Outbox has a clean, extensible API, very few dependencies and plays nicely with a variety of database platforms, transaction management approaches and application frameworks.  Every aspect is highly configurable or overridable.  It features out-of-the-box support for **Spring DI**, **Spring Txn**, **Hibernate**, **Guice**, **MySQL 5 & 8**, **PostgreSQL 9-12** and **H2**.

## Contents
1. [Why do I need it?](#why-do-i-need-it)
1. [Installation](#installation)
1. [Basic Configuration](#basic-configuration)
   1. [No existing transaction manager or dependency injection](#no-existing-transaction-manager-or-dependency-injection)
   1. [Spring](#spring)
   1. [Guice](#guice)
   1. [jOOQ](#jooq)
1. [Set up the background worker](#set-up-the-background-worker)
1. [Managing the "dead letter queue"](#managing-the-dead-letter-queue)
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

 - `TransactionOutbox` creates a proxy of `MessageQueue`. Any method calls on the proxy are serialized and written to a database table _in the same transaction_ as the `SaleRepository` call. The call returns immediately rather than actually invoking the real method.
 - If the transaction rolls back, so do the serialized requests.
 - Immediately after the transaction is successfully committed, another thread will attempt to make the _real_ call to `MessageQueue` asynchronously.
 - If that call fails, or the application dies before the call is attempted, a background "mop-up" thread will re-attempt the call a configurable number of times, with configurable time between each, before **blacklisting** the request and firing and event for it to be investigated (similar to a [dead letter queue](https://en.wikipedia.org/wiki/Dead_letter_queue)).
 - Blacklisted requests can be easily **whitelisted** again once the underlying issue is resolved.

Our service is now resilient and explicitly eventually consistent, as long as all three elements (`SaleRepository` and the downstream event handlers) are idempotent, since those messages will be attempted repeatedly until confirmed successful, which means they could occur multiple times.

If you find yourself wondering _why bother with the queues now_? You're quite right. As we now have outgoing buffers, we already have most of the benefits of middleware (at least for some use cases). We could replace the calls to a message queue with direct queues to the other services' load balancers and switch to a peer-to-peer architecture, if we so choose.

> Note that for the above example to work, `StockReductionEvent` and `IncomeEvent` need to be whitelisted for serialization. See [Configuration reference](#configuration-reference).

## Installation

Requires at least Java 11. **Stuck on an earlier JDK? [Speak up](https://github.com/gruelbox/transaction-outbox/issues/new/choose)**. If there's any interest in downgrading, it won't be particularly hard to strip out the Java 9/10/11 features like `var`.

The latest stable release is available from Maven Central. Check the [latest version](https://github.com/gruelbox/transaction-outbox/releases) and add the dependency:
```xml
<dependency>
  <groupId>com.gruelbox</groupId>
  <artifactId>transactionoutbox-core</artifactId>
  <version>version (e.g. 0.1.4)</version>
</dependency>
```
The following additional artifact plugins are also available (more details below):

 - transactionoutbox-spring
 - transactionoutbox-guice
 - transactionoutbox-jooq

Maven Central is updated regularly. Alternatively, you can follow bleeding-edge continuously-delivered releases from Github Package Repository. This is equivalent to a `SNAPSHOT`, except that you can safely build production releases against it without the risk of it changing.
```xml
<repositories>
  <repository>
    <id>github-transaction-outbox</id>
    <name>Gruelbox Github Repository</name>
    <url>https://maven.pkg.github.com/gruelbox/transaction-outbox</url>
  </repository>
</repositories>
```

## Basic Configuration

An application needs a single, shared instance of `TransactionOutbox`, which is configured using a builder on construction. This takes some time to get right, particularly if you already have a transaction management solution in your application.

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
Alternatively, you could create the `TransactionManager` from a `DataSource`, allowing you to use a connection pooling `DataSource` such as Hikari:

```java
TransactionManager transactionManager = TransactionManager.fromDataSource(dataSource);
```

In this default configuration, `MyClass` must have a default constructor so the "real" implementation can be constructed at the point the method is actually invoked (which might be on another day on another instance of the application). However, you can avoid this requirement by providing an `Instantiator` on every instance of your application that knows how to create the objects:
```java
TransactionOutbox outbox = TransactionOutbox.builder()
  .instantiator(Instantiator.using(clazz -> createInstanceOf(clazz)))
  .build();
```

### Spring

With the out-of-the-box Spring integration, you can let Spring take over both the transaction management _and_ the role of the `Instantiator`. Add the `transactionoutbox-spring` dependency and create your `TransactionOutbox` as a bean:
```java
@Bean
@Lazy
public TransactionOutbox transactionOutbox(SpringTransactionOutboxFactory factory) {
  return factory.create()
      .persistor(Persistor.forDialect(Dialect.H2))
      .build();
}

...

@Transactional
public void doStuff() {
  customerRepository.save(new Customer(1L, "Martin", "Carthy"));
  customerRepository.save(new Customer(2L, "Dave", "Pegg"));
  outbox.get().schedule(getClass()).publishCustomerCreatedEvent(1L);
  outbox.get().schedule(getClass()).publishCustomerCreatedEvent(2L);
}

void publishCustomerCreatedEvent(long id) {
  // Remote call here
}
```
Notice that with a DI framework like Spring in play, you can **self-invoke** on `getClass()` - invoke a method on the same class that's scheduling it.

### Guice

To use Guice for DI instead, add a dependency on `transactionoutbox-guice`, and "inject the injector":
```java
@Provides
@Singleton
TransactionOutbox transactionOutbox(Injector injector, TransactionManager transactionManager) {
  return TransactionOutbox.builder()
    .transactionManager(transactionManager)
    .persistor(Persistor.forDialect(Dialect.MY_SQL_8))
    .instantiator(GuiceInstantiator.builder().injector(injector).build())
    .build();
}
```

### jOOQ

Like Transaction Outbox, jOOQ is intended to play nicely with any other transaction management approach, but provides its own as an option. If you are already using jOOQ's `TransactionProvider` via `DSLContext.transaction(...)` throughout your application, you can continue to do so.

```java
// Configure jOOQ to use thread-local transaction management
var jooqConfig = new DefaultConfiguration();
var connectionProvider = new DataSourceConnectionProvider(dataSource);
jooqConfig.setConnectionProvider(connectionProvider);
jooqConfig.setSQLDialect(SQLDialect.H2);
jooqConfig.setTransactionProvider(new ThreadLocalTransactionProvider(connectionProvider, true));

// Add the outbox listener to synchronise the two transaction managers
var jooqTransactionListener = JooqTransactionManager.createListener();
jooqConfig.set(listener);
var dsl = DSL.using(jooqConfig);

// Create the outbox
var outbox = TransactionOutbox.builder()
    .transactionManager(JooqTransactionManager.create(dsl, listener))
    .persistor(Persistor.forDialect(Dialect.MY_SQL_8))
    .build();
}

// Use jOOQ and Transaction Outbox together
dsl.transaction(ctx -> {
  customerDao.save(new Customer(1L, "Martin", "Carthy"));
  customerDao.save(new Customer(2L, "Dave", "Pegg"));
  outbox.schedule(MyClass.class).publishCustomerCreatedEvent(1L);
  outbox.schedule(MyClass.class).publishCustomerCreatedEvent(2L);
});
```

## Set up the background worker

At the moment, if any work fails first time, it won't be retried.  All we need to add is a background task that repeatedly calls `TransactionOutbox.flush()`, e.g:
```java
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(outbox::flush, 2, 2, TimeUnit.MINUTES);
```
Or with RxJava:
```java
Observable.interval(2, MINUTES).subscribe(i -> outbox.flush());
```
Wire this into your app in whichever way works best. Don't worry about it running on multiple instances simultaneously. It's designed to handle this, and indeed it can be a benefit; spreading high workloads across instances without any need for more complex high-availability configuration (that said, if you want to distribute work across a cluster at point of submission, this is also supported).

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
To mark the work for reprocessing, just use `TransactionOutbox.whitelist()`. Its failure count will be marked back down to zero and it will get reprocessed on the next call to `flush()`:
```
transactionOutboxEntry.whitelist(entryId);
```
A good approach here is to use the `TransactionOutboxListener` callback to post an [interactive Slack message](https://api.slack.com/legacy/interactive-messages) - this can operate as both the alert and the "button" allowing a support engineer to submit the work for reprocessing.

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
    // We want to allow the SaleType enum and Money class to be used in arguments (see example below), so let's
    // customise the Persistor a bit. Selecting the right SQL dialect ensures that features such as SKIP LOCKED
    // are used correctly.  You can create a fully-custom Persistor implementation if your persistence requirements
    // are significantly different, and DefaultPersistor is designed to be extended if you only wish to modify
    // small areas.
    .persistor(DefaultPersistor.builder()
        .dialect(Dialect.POSTGRESQL_9)
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

Instead, stubs exist for the various arguments to the builder, allowing you to build a `TransactionOutbox` with minimal external dependencies which can be called and verified in tests.
```java
// GIVEN
SomeService mockService = Mockito.mock(SomeService.class);
StubTransactionManager transactionManager = StubTransactionManager.builder().build();
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
