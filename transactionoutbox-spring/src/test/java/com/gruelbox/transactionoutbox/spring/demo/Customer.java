package com.gruelbox.transactionoutbox.spring.demo;

import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
class Customer {

  @Id private Long id;
  private String firstName;
  private String lastName;

  protected Customer() {}

  Customer(Long id, String firstName, String lastName) {
    this.id = id;
    this.firstName = firstName;
    this.lastName = lastName;
  }

  @Override
  public String toString() {
    return String.format("Customer[id=%d, firstName='%s', lastName='%s']", id, firstName, lastName);
  }

  Long getId() {
    return id;
  }

  String getFirstName() {
    return firstName;
  }

  String getLastName() {
    return lastName;
  }
}
