# transactionoutbox-quarkus


Extension for [transaction-outbox-core](../README.md) which integrates CDI's DI and Quarkus transaction management.

Tested with Quarkus implementation (Arc/Agroal)

## Installation

### Stable releases

The latest stable release is available from Maven Central.

#### Maven

```xml
<dependency>
  <groupId>com.gruelbox</groupId>
  <artifactId>transactionoutbox-quarkus</artifactId>
  <version>6.0.609</version>
</dependency>
```

#### Gradle

```groovy
implementation 'com.gruelbox:transactionoutbox-quarkus:6.0.609'
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

## Configuration

Create your `TransactionOutbox` as a bean:

```java

@Produces
public TransactionOutbox transactionOutbox(QuarkusTransactionManager transactionManager)
{
   return TransactionOutbox.builder().instantiator(CdiInstantiator.create()).transactionManager(transactionManager).persistor(Persistor.forDialect(Dialect.H2)).build();
}
```

## Usage

```java
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

