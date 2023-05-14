package com.gruelbox.transactionoutbox.spring.demo;

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
class Customer {
  @Id private Long id;
  @Column private String firstName;
  @Column private String lastName;
}
