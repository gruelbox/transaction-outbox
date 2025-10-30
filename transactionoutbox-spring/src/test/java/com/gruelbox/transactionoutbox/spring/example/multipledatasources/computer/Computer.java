package com.gruelbox.transactionoutbox.spring.example.multipledatasources.computer;

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
public class Computer {

  public enum Type {
    LAPTOP, SERVER, DESKTOP;
  }

  @Id
  private Long id;
  @Column
  private String name;
  @Column
  private Type type;
}