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

## Usage
### Creating a `TransactionOutbox`

Create a `TransactionOutbox` using the builder. The configuration is highly flexible and designed to allow you to integrate with any combination of relational DB and transaction management framework. Here are some examples:

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
HikariConfig config = new HikariConfig();
config.setJdbcUrl("jdbc:h2:mem:test;MV_STORE=TRUE");
config.setUsername("test");
config.setPassword("test");
config.addDataSourceProperty("cachePrepStmts", "true");
try (HikariDataSource ds = new HikariDataSource(config)) {
  TransactionManager transactionManager = TransactionManager.fromDataSource(ds);
  TransactionOutbox outbox = TransactionOutbox.builder()
    .transactionManager(transactionManager)
    .dialect(Dialect.H2)
    .build();
  
  ...
  ...
}
```
Or add `transactionoutbox-spring` to your POM and integrate with Spring DI, Spring Tx and JPA:
```
@Bean
@Lazy
public TransactionOutbox transactionOutbox(ApplicationContext applicationContext, EntityManager entityManager) {
  return TransactionOutbox.builder()
    .dialect(Dialect.H2)
    .instantiator(SpringInstantiator.builder().applicationContext(applicationContext).build())
    .transactionManager(SpringTransactionManager.builder().entityManager(entityManager).build())
    .build();
}
```
Perhaps integrate with Guice and jOOQ's transaction management instead?
```
@Provides
@Singleton
DSLContext parentDsl() {
  return DSL.using(
    "jdbc:h2:mem:test;MV_STORE=TRUE",
    "test",
    "test");
}

@Provides
@Singleton
TransactionManager transactionManager(DSLContext dsl) {
  return JooqTransactionManager.builder().parentDsl(dsl).build();
}

@Provides
Dialect dialect(DatabaseConfiguration databaseConfiguration) {
  return databaseConfiguration.getDialect().toTxnOutboxType();
}

@Provides
@Singleton
TransactionOutbox transactionOutbox(Injector injector, Dialect dialect, TransactionManager transactionManager) {
  return TransactionOutbox.builder()
    .dialect(dialect)
    .instantiator(Instantiator.using(injector::getInstance))
    .transactionManager(transactionManager)
    .build();
}
```

### Scheduling work
During a transaction, you can _schedule_ work to be run at some later point in time (usually immediately, but if that fails, potentially some time later, after a number of retries). This instruction is persisted to the database in the same transaction as the rest of your work, giving guaranteed eventual consistency.

In general, this is expressed as:
```
outbox.schedule(ClassToCall.class).methodToCall(arg1, arg2, arg3);
```
This says "at the earliest opportunity, please obtain an instance of `ClassToCall` and call the `methodToCall` method on it, passing the arguments `[ arg1, arg2, arg3]`. If that call fails, try again repeatedly until the configured maximum number of retries".

You must call `TransactionOutbox.schedule()` _within an ongoing database transaction_ which the `TransactionManager` (the one you passed when building the `TransactionOutbox`) is aware of.

If using the built-in transaction manager, you should start a transaction using `TransactionManager.inTransaction()`:

```
transactionManager.inTransaction(() -> {
  // Do some work within your transaction
  // This code MUST use the connection currently active within the `TransactionManager`.
  customerService.createCustomer(transactionManager, customer);
  // Schedule a transaction outbox task
  outbox.schedule(EventPublisher.class).publishEvent(NewCustomerEvent.of(customer));
});
```
However, you can integrate with existing transaction management mechanisms, such as with Spring (using `SpringTransactionManager` as shown above), and ensure all transaction management is performed via Spring's `Transactional` annotation:
```
@Autowired private CustomerRepository customerRepository;
@Autowired private EventRepository eventRepository;
@Autowired @Lazy private TransactionOutbox outbox;

@RequestMapping("/createCustomer")
@Transactional
public String createCustomer() {
  LOGGER.info("Creating customers");
  outbox.schedule(EventRepository.class).save(new Event(1L, "Created customers", LocalDateTime.now()));
  customerRepository.save(new Customer("Joe", "Bloggs"));
  customerRepository.save(new Customer("Billy", "Blue"));
  LOGGER.info("Customers created");
  return "Done";
}
```
See the [Spring example](https://github.com/gruelbox/transaction-outbox/tree/master/transactionoutbox-spring/src/main/java/com/gruelbox/transactionoutbox) to see this in context.

Or jOOQ:

```
dsl.transaction(config -> {
  customerService.createCustomer(config.dsl(), customer);
  outbox.schedule(EventPublisher.class).publishEvent(NewCustomerEvent.of(customer));
});
```

### Injecting dependencies into workers
The default behaviour when you call as follows:
```
outbox.schedule(ClassToCall.class).methodToCall(arg1, arg2, arg3);
```
is to attempt to obtain an instance of `ClassToCall` via reflection, assuming there is a no-args constructor. This obviously doesn't play well with dependency injection.

`SpringInstantiator`, as used above, will instead use Spring's `ApplicationContext.getBean()` method to obtain the object, allowing injection into it, and the Guice example will use `Injector.getInstance()`.

### Ensuring work is processed eventually

To ensure that any scheduled work that fails first time is eventually retried, create a background task (which can run on multiple application instances) which repeatedly calls `TransactionOutbox.flush()`.  That's it!  Example:
```
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(outbox::flush, 2, 2, TimeUnit.MINUTES);
```

### Managing the "dead letter queue"
Work might be retried too many times and get `blacklisted`. You should set up an alert to allow you to manage this when it occurs, resolve the issue and un-blacklist the work, since the work not being complete will usually be a sign that your system is out of sync in some way.

TODO add APIs for this

## How it works

TODO

## Configuration options

TODO
