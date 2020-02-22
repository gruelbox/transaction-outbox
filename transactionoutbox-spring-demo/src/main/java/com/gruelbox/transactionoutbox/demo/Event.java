package com.gruelbox.transactionoutbox.demo;

import java.time.LocalDateTime;
import javax.persistence.Entity;
import javax.persistence.Id;

@Entity
class Event {

  @Id private Long id;

  private String description;
  private LocalDateTime created;

  protected Event() {}

  public Event(long id, String description, LocalDateTime created) {
    this.id = id;
    this.description = description;
    this.created = created;
  }

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public LocalDateTime getCreated() {
    return created;
  }

  public void setCreated(LocalDateTime created) {
    this.created = created;
  }
}
