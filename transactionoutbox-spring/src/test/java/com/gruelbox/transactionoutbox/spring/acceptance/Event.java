package com.gruelbox.transactionoutbox.spring.acceptance;

import java.time.LocalDateTime;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
class Event {

  @Id private Long id;

  private String description;
  private LocalDateTime created;

  protected Event() {}

  Event(long id, String description, LocalDateTime created) {
    this.id = id;
    this.description = description;
    this.created = created;
  }

  Long getId() {
    return id;
  }

  void setId(Long id) {
    this.id = id;
  }

  String getDescription() {
    return description;
  }

  void setDescription(String description) {
    this.description = description;
  }

  LocalDateTime getCreated() {
    return created;
  }

  void setCreated(LocalDateTime created) {
    this.created = created;
  }
}
