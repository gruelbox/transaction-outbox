package com.gruelbox.transactionoutbox;

import jakarta.validation.ClockProvider;
import jakarta.validation.Validation;
import jakarta.validation.ValidationException;

class Validator {

  private final jakarta.validation.Validator validator;

  Validator(ClockProvider clockProvider) {
    this.validator =
        Validation.byDefaultProvider()
            .configure()
            .clockProvider(clockProvider)
            .buildValidatorFactory()
            .getValidator();
  }

  void validate(Object object) {
    var validationErrors = validator.validate(object);
    if (!validationErrors.isEmpty()) {
      throw new ValidationException(
          "Validation on " + object.toString() + " failed: " + validationErrors);
    }
  }
}
