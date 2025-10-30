package com.gruelbox.transactionoutbox.spring.example.multipledatasources.employee;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Employee {
  @Id
  private Long id;
  @Column
  private String firstName;
  @Column private String lastName;
}