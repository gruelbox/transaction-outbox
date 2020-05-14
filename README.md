# transaction-outbox
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

Note that a few bits aren't quire ready, notably: (a) test coverage isn't as high as it should be, (b) it's not had any production "battle testing" and (c) recovery from blacklistings isn't yet implemented. However, please have a play and get tback to me with feedback!

Currently, releases are available as continuously-delivered releases via the Github Package repository. Less regular, stable releases will be performed to Maven Central soon.  Add this to your POM:
```
<repositories>
  <repository>
    <id>github-transaction-outbox</id>
    <name>Gruelbox Github Repository</name>
    <url>https://maven.pkg.github.com/gruelbox/transaction-outbox</url>
  </repository>
</repositories>
```
Check the [latest version](https://github.com/gruelbox/transaction-outbox/packages/159210) and add the dependency:
```
<dependency>
  <groupId>com.gruelbox</groupId>
  <artifactId>transactionoutbox-core</artifactId>
  <version>version (e.g. 0.1.4)</version>
</dependency>
```
Plus this if you are using Spring with JPA:
```
<dependency>
  <groupId>com.gruelbox</groupId>
  <artifactId>transactionoutbox-spring</artifactId>
  <version>version (e.g. 0.1.4)</version>
</dependency>
```
Or this if you're using Guice:
```
<dependency>
  <groupId>com.gruelbox</groupId>
  <artifactId>transactionoutbox-guice</artifactId>
  <version>version (e.g. 0.1.4)</version>
</dependency>
```
Or this if you're using JOOQ's built-in transaction management:
```
<dependency>
  <groupId>com.gruelbox</groupId>
  <artifactId>transactionoutbox-jooq</artifactId>
  <version>version (e.g. 0.1.4)</version>
</dependency>
```

## Creating a `TransactionOutbox`

Create a `TransactionOutbox` using the builder. Instances are thread-safe - only a single instance is required, although you can create more.

The configuration is highly flexible and designed to allow you to integrate with any combination of relational DB and transaction management framework. Here are some examples:

Super-simple - if you have no existing transaction management, connection pooling or dependency injection:
```
TransactionManager transactionManager = TransactionManager.fromConnectionDetails(
    "org.h2.Driver", "jdbc:h2:mem:test;MV_STORE=TRUE", "test", "test"))
TransactionOutbox outbox = TransactionOutbox.builder()
  .transactionManager(transactionManager)
  .persistor(Persistor.forDialect(Dialect.H2))
  .build();
```
Better - use connection pooling:
```
try (HikariDataSource ds = new HikariDataSource(createHikariConfig())) {
  TransactionManager transactionManager = TransactionManager.fromDataSource(ds);
  TransactionOutbox outbox = TransactionOutbox.builder()
    .transactionManager(transactionManager)
    .persistor(Persistor.forDialect(Dialect.H2))
    .build();
}
```
Or add `transactionoutbox-spring` to your POM and integrate with Spring DI, Spring Tx and JPA:
```
@Bean
@Lazy
public TransactionOutbox transactionOutbox(SpringTransactionOutboxFactory factory) {
  return factory.create()
      .persistor(Persistor.forDialect(Dialect.H2))
      .build();
}
```
Perhaps integrate with Guice and jOOQ's transaction management instead? It's designed to permit all sorts of combinations of libraries:
```
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
  jooqConfig.setTransactionProvider(new ThreadLocalTransactionProvider(connectionProvider));
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
```
outbox.schedule(ClassToCall.class).methodToCall(arg1, arg2, arg3);
```
This means:

> At the earliest opportunity, please obtain an instance of `ClassToCall` and call the `methodToCall` method on it, passing the arguments `[ arg1, arg2, arg3 ]`. If that call fails, try again repeatedly until the configured maximum number of retries".

You must call `TransactionOutbox.schedule()` _within an ongoing database transaction_, and the `TransactionManager` 
(the one you passed when building the `TransactionOutbox`) needs to be aware of it.

### Using built-in transaction handling

If using the built-in transaction manager, you should start a transaction using `TransactionManager.inTransaction()`:

```
transactionManager.inTransaction(tx -> {
  // Do some work using the transaction
  customerService.createCustomer(tx, customer);
  // Schedule a transaction outbox task (automatically uses the same transaction)
  outbox.schedule(EventPublisher.class).publishEvent(NewCustomerEvent.of(customer));
});
```

### Using spring-txn
If you're using Spring transaction management, you can use the `Transactional` annotation as normal:
```
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

```
dsl.transaction(config -> {
  // Do some work using the transaction
  customerService.createCustomer(config.dsl(), customer);
  // Schedule a transaction outbox task (automatically uses the same transaction)
  outbox.schedule(EventPublisher.class).publishEvent(NewCustomerEvent.of(customer));
});
```

## Dependency injection
The default behaviour when you schedule work as follows:
```
outbox.schedule(ClassToCall.class).methodToCall(arg1, arg2, arg3);
```
is to attempt to obtain an instance of `ClassToCall` via reflection, assuming there is a no-args constructor. This 
obviously doesn't play well with dependency injection.

`SpringInstantiator`, as used above, will instead use Spring's `ApplicationContext.getBean()` method to obtain the object,
allowing injection into it, and the Guice example will use `Injector.getInstance()`. If you have some other DI mechanism,
simply create your own implementation of `Instantiator` and pass it when building the `TransactionalOutbox`:
```
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
```
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(outbox::flush, 2, 2, TimeUnit.MINUTES);
```
That's it! Just make sure that this process keeps running, or schedule it repeatedly using message queues or a scheduler such as Quartz.

## Managing the "dead letter queue"
Work might be retried too many times and get "blacklisted". You should set up an alert to allow you to manage this when it occurs, resolve the issue and un-blacklist the work, since the work not being complete will usually be a sign that your system is out of sync in some way.
```
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

TODO

## Stubbing

`TransactionOutbox` is not intended to be stubbed; the underlying logic is too onerous to stub out using a a mocking framework such as Mockito. Instead, stubs exist for the various arguments to the builder:
```
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
        .transactionManager(transactionManager)
        .build();
```
Depending on the type of test, you may wish to use a real `Persistor` such as `DefaultPersistor` (if there's a real database available) or a real, multi-threaded `Submitter`. That's up to you.
