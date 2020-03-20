# transaction-outbox
![](https://github.com/gruelbox/transaction-outbox/workflows/Java%20CI%20with%20Maven/badge.svg)

[Transaction Outbox Pattern](https://microservices.io/patterns/data/transactional-outbox.html) implementation for Java. Features:

 - Clean, extensible API
 - Very few dependencies
 - Plays nicely with a variety of database platforms, transaction management approaches and application frameworks
 
 Direct support for the following, and easily extended to support others:
 
 - Spring
 - Hibernate
 - Guice
 - MySQL 5 & 8
 - PostgreSQL 9, 10, 11, 12
 - H2

## Installation

It's yet quite ready for Maven Central release, so if you want to have a play, you'll need to use JitPack. Add this to your POM:
```
<repositories>
  <repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
  </repository>
</repositories>
```
And add this dependency:
```
<dependency>
  <groupId>com.github.gruelbox.transaction-outbox</groupId>
  <artifactId>transactionoutbox-core</artifactId>
  <version>master-SNAPSHOT</version>
</dependency>
```
Plus this if you are using Spring with JPA:
```
<dependency>
  <groupId>com.github.gruelbox.transaction-outbox</groupId>
  <artifactId>transactionoutbox-spring</artifactId>
  <version>master-SNAPSHOT</version>
</dependency>
```
Or this if you're using Guice:
```
<dependency>
  <groupId>com.github.gruelbox.transaction-outbox</groupId>
  <artifactId>transactionoutbox-guice</artifactId>
  <version>master-SNAPSHOT</version>
</dependency>
```
Or this if you're using JOOQ's built-in transaction management:
```
<dependency>
  <groupId>com.github.gruelbox.transaction-outbox</groupId>
  <artifactId>transactionoutbox-jooq</artifactId>
  <version>master-SNAPSHOT</version>
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
  .dialect(Dialect.H2)
  .build();
```
Better - use connection pooling:
```
try (HikariDataSource ds = new HikariDataSource(createHikariConfig())) {
  TransactionManager transactionManager = TransactionManager.fromDataSource(ds);
  TransactionOutbox outbox = TransactionOutbox.builder()
    .transactionManager(transactionManager)
    .dialect(Dialect.H2)
    .build();
}
```
Or add `transactionoutbox-spring` to your POM and integrate with Spring DI, Spring Tx and JPA:
```
@Bean
@Lazy
public TransactionOutbox transactionOutbox(SpringTransactionOutboxFactory factory) {
  return factory.create()
      .dialect(Dialect.H2)
      .build();
}
```
Perhaps integrate with Guice and jOOQ's transaction management instead? It's designed to permit all sorts of combinations of libraries:
```
@Provides
@Singleton
Configuration jooqConfig(DataSource dataSource) {
  DefaultConfiguration jooqConfig = new DefaultConfiguration();
  DataSourceConnectionProvider connectionProvider = new DataSourceConnectionProvider(dataSource);
  jooqConfig.setConnectionProvider(connectionProvider);
  jooqConfig.setSQLDialect(SQLDialect.H2);
  jooqConfig.setTransactionProvider(new ThreadLocalTransactionProvider(connectionProvider));
  return jooqConfig;
}

@Provides
@Singleton
JooqTransactionListener jooqListener(Configuration jooqConfig) {
  return JooqTransactionManager.createListener(jooqConfig);
}

@Provides
@Singleton
DSLContext parentDsl(Configuration jooqConfig, JooqTransactionListener listener) {
  return DSL.using(configuration);
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
    .dialect(Dialect.MY_SQL_8)
    .instantiator(Instantiator.using(injector::getInstance))
    .transactionManager(transactionManager)
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
```$
TransactionOutbox.builder()
    .dialect(Dialect.POSTRESQL_9)
    .instantiator(new Instantiator() {
       ...
     })
    .transactionManager(transactionManager)
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

TODO add APIs for this

## How it works

TODO

## Configuration options

TODO
