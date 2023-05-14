package com.gruelbox.transactionoutbox.acceptance;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.time.LocalDateTime;
import lombok.Data;

@Entity
@Data
class Event {
  @Id private Long id;
  @Column private String description;
  @Column private LocalDateTime created;
}
