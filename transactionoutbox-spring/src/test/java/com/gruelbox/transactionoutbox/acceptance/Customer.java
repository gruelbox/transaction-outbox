package com.gruelbox.transactionoutbox.acceptance;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import lombok.Data;

@Entity
@Data
class Customer {
  @Id private Long id;
  @Column private String firstName;
  @Column private String lastName;
}
