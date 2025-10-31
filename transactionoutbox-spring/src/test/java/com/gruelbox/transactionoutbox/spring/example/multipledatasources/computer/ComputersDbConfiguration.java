package com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer;

import jakarta.persistence.EntityManagerFactory;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableJpaRepositories(
    basePackageClasses = Computer.class,
    entityManagerFactoryRef = "computerEntityManager",
    transactionManagerRef = "computerTransactionManager")
public class ComputersDbConfiguration {

  @Bean
  public DataSourceProperties computerDataSourceProperties() {
    DataSourceProperties properties = new DataSourceProperties();
    properties.setDriverClassName(org.h2.Driver.class.getName());
    properties.setUrl("jdbc:h2:mem:computer");
    properties.setUsername("computerUser");
    properties.setPassword("computerPassword");
    return properties;
  }

  @Bean
  public DataSource computerDataSource() {
    return computerDataSourceProperties().initializeDataSourceBuilder().build();
  }

  @Bean
  public JdbcTemplate computerJdbcTemplate() {
    return new JdbcTemplate(computerDataSource());
  }

  @Bean
  public LocalContainerEntityManagerFactoryBean computerEntityManager() {
    LocalContainerEntityManagerFactoryBean emf = new LocalContainerEntityManagerFactoryBean();
    emf.setDataSource(computerDataSource());
    emf.setPackagesToScan(Computer.class.getPackage().getName());
    emf.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
    emf.setJpaPropertyMap(
        Map.of(
            "hibernate.hbm2ddl.auto", "update",
            "hibernate.show_sql", "true"));
    emf.setPersistenceUnitName("computer");
    return emf;
  }

  @Bean
  public PlatformTransactionManager computerTransactionManager(
      @Qualifier("computerEntityManager") EntityManagerFactory entityManagerFactory) {
    return new JpaTransactionManager(entityManagerFactory);
  }
}
