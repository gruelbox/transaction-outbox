package com.gruelbox.transactionoutbox;

public interface Validatable {
  void validate(Validator validator);
}
