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
Create a `TransactionOutbox` using the builder. The configuration is highly flexible and designed to allow you to integrate with any combination of relational DB and transaction management framework. Here are some examples:

Super-simple - if you have no existing transaction management, connection pooling or dependency injection:
```
TransactionManager transactionManager = TransactionManager.fromConnectionDetails(
    "org.h2.Driver", "jdbc:h2:mem:test;MV_STORE=TRUE", "test", "test"))
TransactionOutbox outbox = TransactionOutbox.builder()
  .transactionManager(transactionManager)
  .dialect(Dialect.H2)
  .build();
transactionManager.inTransaction(() -> {
  // Do some work within your transaction
  customerService.createCustomer(transactionManager, customer);
  // Schedule a transaction outbox task
  outbox.schedule(EventPublisher.class).publishEvent(NewCustomerEvent.of(customer));
});
  
  
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
  transactionManager.inTransaction(() -> {
    // Do some work within your transaction
    customerService.createCustomer(transactionManager, customer);
    // Schedule a transaction outbox task
    outbox.schedule(EventPublisher.class).publishEvent(NewCustomerEvent.of(customer));
  });
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

...

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

## How it works

TODO

## Configuration options

TODO
