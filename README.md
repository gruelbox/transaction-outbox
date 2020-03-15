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
 - MySQL
 - PostgreSQL
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
config.setJdbcUrl("jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;DEFAULT_LOCK_TIMEOUT=60000;LOB_TIMEOUT=2000;MV_STORE=TRUE");
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
### Scheduling work
During a transaction, you can _schedule_ work to be run at some later point in time (usually immediately, but if that fails, potentially some time later, after a number of retries). This instruction is persisted to the database in the same transaction as the rest of your work, giving guaranteed eventual consistency.

In general, this is expressed as:
```
outbox.schedule(ClassToCall.class).methodToCall(arg1, arg2, arg3);
```
This says "at the earliest opportunity, please obtain an instance of `ClassToCall` and call the `methodToCall` method on it, passing the arguments `[ arg1, arg2, arg3]`. If that call fails, try again repeatedly until the configured maximum number of retries".

If using the built-in transaction manager:

```
transactionManager.inTransaction(() -> {
  // Do some work within your transaction
  customerService.createCustomer(transactionManager, customer);
  // Schedule a transaction outbox task
  outbox.schedule(EventPublisher.class).publishEvent(NewCustomerEvent.of(customer));
});
```
However, you can integrate with existing transaction management mechanisms, such as with Spring (using `SpringTransactionManager` as shown above):
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
### Ensuring work is processed eventually

To ensure that any scheduled work that fails first time is eventually retried, create a background task (which can run on multiple application instances) which repeatedly calls `TransactionOutbox.flush()`.  That's it!  Example:
```
ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
scheduler.scheduleAtFixedRate(() -> {
    if (Thread.interrupted()) return;
    outbox.flush();
  },
  2, 2, TimeUnit.MINUTES);
```
### Managing the "dead letter queue"
Work might be retried too many times and get `blacklisted`. You should set up an alert to allow you to manage this when it occurs, resolve the issue and un-blacklist the work, since the work not being complete will usually be a sign that your system is out of sync in some way.

TODO add APIs for this

## How it works

TODO

## Configuration options

TODO
