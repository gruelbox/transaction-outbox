package com.gruelbox.transactionoutbox;

import javax.validation.ClockProvider;
import javax.validation.Validation;
import javax.validation.ValidationException;

class Validator {

    private final javax.validation.Validator validator;

    Validator(ClockProvider clockProvider) {
        this.validator = Validation.byDefaultProvider()
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
