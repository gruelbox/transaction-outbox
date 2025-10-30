package com.gruelbox.transactionoutbox.spring.example.multipledatasources.employee;

import jakarta.persistence.EntityManagerFactory;
import java.util.Map;
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@EnableJpaRepositories(
    basePackageClasses = Employee.class,
    entityManagerFactoryRef = "employeeEntityManager",
    transactionManagerRef = "employeeTransactionManager"
)
public class EmployeesDbConfiguration {

  @Bean
  @ConfigurationProperties("spring.datasource.employees")
  public DataSourceProperties employeeDataSourceProperties() {
    DataSourceProperties properties = new DataSourceProperties();
    properties.setDriverClassName(org.h2.Driver.class.getName());
    properties.setUrl("jdbc:h2:mem:employee");
    properties.setUsername("employeeUser");
    properties.setPassword("employeePassword");
    return properties;
  }

  @Bean
  public DataSource employeeDataSource() {
    return employeeDataSourceProperties().initializeDataSourceBuilder().build();
  }

  @Bean
  public LocalContainerEntityManagerFactoryBean employeeEntityManager() {
    LocalContainerEntityManagerFactoryBean emf = new LocalContainerEntityManagerFactoryBean();
    emf.setDataSource(employeeDataSource());
    emf.setPackagesToScan(Employee.class.getPackage().getName());
    emf.setJpaVendorAdapter(new HibernateJpaVendorAdapter());
    emf.setJpaPropertyMap(Map.of(
        "hibernate.hbm2ddl.auto", "update",
        "hibernate.show_sql", "true"));
    emf.setPersistenceUnitName("employee");
    return emf;
  }

  @Bean
  public PlatformTransactionManager employeeTransactionManager(
      @Qualifier("employeeEntityManager") EntityManagerFactory entityManagerFactory) {
    return new JpaTransactionManager(entityManagerFactory);
  }
}
