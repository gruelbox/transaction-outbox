# transaction-outbox
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transaction-outbox/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.gruelbox/transaction-outbox)
[![Javadocs](https://www.javadoc.io/badge/com.gruelbox/transaction-outbox.svg?color=blue)](https://www.javadoc.io/doc/com.gruelbox/transaction-outbox)
[![CD](https://github.com/gruelbox/transaction-outbox/workflows/Continous%20Delivery/badge.svg)](https://github.com/gruelbox/transaction-outbox/actions)

## Contents
1. [What's this?](#whats-this)
1. [Why do I need it?](#why-do-i-need-it)
1. [How does it work?](#how-does-it-work)
1. [Installation](#installation)
1. [Creating a `TransactionOutbox`](#creating-a-transactionoutbox)
1. [Usage](#usage)
   1. [Using built-in transaction handling](#using-built-in-transaction-handling)
   1. [Using spring-txn](#using-spring-txn)
   1. [Using j00Q's built-in transaction management](#using-j00qs-built-in-transaction-management)
1. [Dependency Injection](#dependency-injection)
1. [Ensuring work is processed eventually](#ensuring-work-is-processed-eventually)
1. [Managing the "dead letter queue"](#managing-the-dead-letter-queue)
1. [Configuration options](#configuration-options)
1. [Stubbing](#stubbing)

## What's this?

An implementation of the [Transaction Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html) for Java. Features a clean, extensible API, very few dependencies and plays nicely with a variety of database platforms, transaction management approaches and application frameworks.

## Why do I need it?

Unless you are using [event sourcing](https://martinfowler.com/eaaDev/EventSourcing.html), if you are using Microservices with databases and you have use cases that span more than one service, you will encounter the situation where one service updates its database and then either calls or sends a message to another service. During this process, there are many things that could go wrong, for example:

 - The message queue or other service could be unavailable, or there could be a network glitch at the time the message is sent.
 - If we commit our database transaction _before_ sending the external request, the service might die, freeze or get taken down for upgrade before it has a chance to send the request.
 - If we commit our database transaction _after_ sending the request, the service might die, freeze or get taken down for upgrade before it has a chance to commit the transaction.
 
Naively, we would end up with an _at least once_ solution - we will send the request once or not at all. This is seldom what we want. _Exactly once_ would be ideal, but this is [really hard](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/). What we can do easily is to is achieve _at least once_ semantics, and rely on [idempotency keys](https://stripe.com/gb/blog/idempotency) at the receiving end to "collapse" down to _exactly once_.

The _sending_ side is what a `TransactionOutbox` solves: it ensures that once we have committed our database transaction we _know_ that the corresponding message will either eventually make it to the next system in the chain, or will go into a failure state from which it can be recovered and pushed through.

It solves the primary risk of the "vanilla" transactional outbox pattern by managing both the scheduling and processing of the external call, and also provides 100% flexibility regarding the nature of the external call.

## How does it work?

`TransactionOutbox` uses a table to your application's database (much like [Flyway](https://flywaydb.org/) or [Liquibase](https://www.liquibase.org/)) to record method calls. These are committed with the rest of your database transaction and then processed in the background, repeatedly, until the method call runs without throwing an exception.

Every aspect is highly configurable or overridable. It has direct support for the following, and is easily extended to support others:
 
 - Spring
 - Hibernate
 - Guice
 - MySQL 5 & 8
 - PostgreSQL 9, 10, 11, 12
 - H2

## Installation

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

## Creating a `TransactionOutbox`

Create a `TransactionOutbox` using the builder. Instances are thread-safe - only a single instance is required, although you can create more.

The configuration is highly flexible and designed to allow you to integrate with any combination of relational DB and transaction management framework. Here are some examples:

Super-simple - if you have no existing transaction management, connection pooling or dependency injection:
```java
TransactionManager transactionManager = TransactionManager.fromConnectionDetails(
    "org.h2.Driver", "jdbc:h2:mem:test;MV_STORE=TRUE", "test", "test"))
TransactionOutbox outbox = TransactionOutbox.builder()
  .transactionManager(transactionManager)
  .persistor(Persistor.forDialect(Dialect.H2))
  .build();
```
Better - use connection pooling:
```java
try (HikariDataSource ds = new HikariDataSource(createHikariConfig())) {
  TransactionManager transactionManager = TransactionManager.fromDataSource(ds);
  TransactionOutbox outbox = TransactionOutbox.builder()
    .transactionManager(transactionManager)
    .persistor(Persistor.forDialect(Dialect.H2))
    .build();
}
```
Or add `transactionoutbox-spring` to your POM and integrate with Spring DI, Spring Tx and JPA:
```java
@Bean
@Lazy
public TransactionOutbox transactionOutbox(SpringTransactionOutboxFactory factory) {
  return factory.create()
      .persistor(Persistor.forDialect(Dialect.H2))
      .build();
}
```
Perhaps integrate with Guice and jOOQ's transaction management instead? It's designed to permit all sorts of combinations of libraries:
```java
@Provides
@Singleton
JooqTransactionListener jooqListener() {
  return JooqTransactionManager.createListener();
}

@Provides
@Singleton
DSLContext parentDsl(Configuration jooqConfig, JooqTransactionListener listener) {
  DefaultConfiguration jooqConfig = new DefaultConfiguration();
  DataSourceConnectionProvider connectionProvider = new DataSourceConnectionProvider(dataSource);
  jooqConfig.setConnectionProvider(connectionProvider);
  jooqConfig.setSQLDialect(SQLDialect.H2);
  jooqConfig.setTransactionProvider(new ThreadLocalTransactionProvider(connectionProvider, true));
  jooqConfig.set(listener);
  return DSL.using(jooqConfig);
}

@Provides
@Singleton
TransactionManager transactionManager(DSLContext dsl, JooqTransactionListener listener) {
  return JooqTransactionManager.create(dsl, listener);
}

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

## Usage

During a transaction, you can _schedule_ work to be run at some later point in time (usually immediately, but if that fails, potentially some time later, after a number of retries). This instruction is persisted to the database in the same transaction as the rest of your work, giving guaranteed eventual consistency.

In general, this is expressed as:
```java
outbox.schedule(ClassToCall.class).methodToCall(arg1, arg2, arg3);
```
This means:

> At the earliest opportunity, please obtain an instance of `ClassToCall` and call the `methodToCall` method on it, passing the arguments `[ arg1, arg2, arg3 ]`. If that call fails, try again repeatedly until the configured maximum number of retries".

You must call `TransactionOutbox.schedule()` _within an ongoing database transaction_, and the `TransactionManager` 
(the one you passed when building the `TransactionOutbox`) needs to be aware of it.

### Using built-in transaction handling

If using the built-in transaction manager, you should start a transaction using `TransactionManager.inTransaction()`:

```java
transactionManager.inTransaction(tx -> {
  // Do some work using the transaction
  customerService.createCustomer(tx, customer);
  // Schedule a transaction outbox task (automatically uses the same transaction)
  outbox.schedule(EventPublisher.class).publishEvent(NewCustomerEvent.of(customer));
});
```

### Using spring-txn
If you're using Spring transaction management, you can use the `Transactional` annotation as normal:
```java
@Autowired private CustomerRepository customerRepository;
@Autowired private Provider<TransactionOutbox> outbox;
@Autowired private EventRepository eventRepository;
@Autowired private EventPublisher eventPublisher;

@Transactional // Starts the transaction
public void createCustomer() {
  // Do some work using the transaction
  customerRepository.save(new Customer(1L, "Martin", "Carthy"));
  customerRepository.save(new Customer(2L, "Dave", "Pegg"));
  // Schedule a transaction outbox task (automatically uses the same transaction)
  outbox.get().schedule(eventPublisher.getClass()).publish(new Event(1L, "Created customers", LocalDateTime.now()));
}
```
See the [Spring example](https://github.com/gruelbox/transaction-outbox/tree/master/transactionoutbox-spring/src/test/java/com/gruelbox/transactionoutbox/acceptance/TransactionOutboxSpringDemoApplication.class) to see this in context.

### Using j00Q's built-in transaction management

You can use `DSLContext.transaction()` as normal:

```java
dsl.transaction(config -> {
  // Do some work using the transaction
  customerService.createCustomer(config.dsl(), customer);
  // Schedule a transaction outbox task (automatically uses the same transaction)
  outbox.schedule(EventPublisher.class).publishEvent(NewCustomerEvent.of(customer));
});
```

## Dependency injection
The default behaviour when you schedule work as follows:
```java
outbox.schedule(ClassToCall.class).methodToCall(arg1, arg2, arg3);
```
is to attempt to obtain an instance of `ClassToCall` via reflection, assuming there is a no-args constructor. This 
obviously doesn't play well with dependency injection.

`SpringInstantiator`, as used above, will instead use Spring's `ApplicationContext.getBean()` method to obtain the object,
allowing injection into it, and the Guice example will use `Injector.getInstance()`. If you have some other DI mechanism,
simply create your own implementation of `Instantiator` and pass it when building the `TransactionalOutbox`:
```java
TransactionOutbox.builder()
    .transactionManager(transactionManager)
    .persistor(Persistor.forDialect(Dialect.POSTRESQL_9))
    .instantiator(new Instantiator() {
       ...
     })
    .build();
```

## Ensuring work is processed eventually

To ensure that any scheduled work that fails first time is eventually retried, create a background task (which can run on multiple application instances) which repeatedly calls `TransactionOutbox.flush()`, e.g:
```java
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(outbox::flush, 2, 2, TimeUnit.MINUTES);
```
That's it! Just make sure that this process keeps running, or schedule it repeatedly using message queues or a scheduler such as Quartz.

## Managing the "dead letter queue"
Work might be retried too many times and get "blacklisted". You should set up an alert to allow you to manage this when it occurs, resolve the issue and un-blacklist the work, since the work not being complete will usually be a sign that your system is out of sync in some way.
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

## Configuration options

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

## Stubbing

`TransactionOutbox` is not intended to be stubbed; the underlying logic is too onerous to stub out using a a mocking framework such as Mockito. Instead, stubs exist for the various arguments to the builder:
```java
// This ensures that all the commit hooks are called at the right times, which is very hard to stub manually
StubTransactionManager transactionManager = StubTransactionManager.builder().build();

// Direct pass-through
TransactionOutbox outbox = TransactionOutbox.builder()
        .instantiator(Instantiator.using(clazz -> {
            assertEquals(WhatIExpectToBeCalled.class, clazz);
            return theObjectToBeCalled; // This can be a mock
        }))
        .persistor(StubPersistor.builder().build()) // Doesn't save anything
        .submitter(Submitter.withExecutor(MoreExecutors.directExecutor())) // Execute all work in-line
        .clockProvider(() ->
            Clock.fixed(LocalDateTime.of(2020, 3, 1, 12, 0)
                .toInstant(ZoneOffset.UTC), ZoneOffset.UTC)) // Fix the clock
        .transactionManager(transactionManager)
        .build();
```
Depending on the type of test, you may wish to use a real `Persistor` such as `DefaultPersistor` (if there's a real database available) or a real, multi-threaded `Submitter`. That's up to you.
