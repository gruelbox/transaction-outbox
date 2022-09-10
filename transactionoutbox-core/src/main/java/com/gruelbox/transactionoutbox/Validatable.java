package com.gruelbox.transactionoutbox;

interface Validatable {
  void validate(Validator validator);
}
