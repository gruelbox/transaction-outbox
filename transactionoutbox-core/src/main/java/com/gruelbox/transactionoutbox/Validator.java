package com.gruelbox.transactionoutbox;

import java.time.Clock;
import java.time.Instant;
import java.util.function.Supplier;

class Validator {

    private final String path;
    private final Supplier<Clock> clockProvider;

    Validator(Supplier<Clock> clockProvider) {
        this.clockProvider = clockProvider;
        this.path = "";
    }

    private Validator(String className, Validator validator) {
        this.clockProvider = validator.clockProvider;
        this.path = className;
    }

    public void validate(Validatable validatable) {
        validatable.validate(new Validator(validatable.getClass().getSimpleName(), this));
    }

    public void valid(String propertyName, Object object) {
        notNull(propertyName, object);
        if (!(object instanceof Validatable)) {
            return;
        }
        ((Validatable) object)
                .validate(new Validator(path.isEmpty() ? propertyName : (path + "." + propertyName), this));
    }

    public void notNull(String propertyName, Object object) {
        if (object == null) {
            error(propertyName, "may not be null");
        }
    }

    public void nullOrNotBlank(String propertyName, String object) {
        if (object != null && object.isEmpty()) {
            error(propertyName, "may be either null or non-blank");
        }
    }

    public void notBlank(String propertyName, String object) {
        notNull(propertyName, object);
        if (object.isEmpty()) {
            error(propertyName, "may not be blank");
        }
    }

    public void inFuture(String propertyName, Instant object) {
        notNull(propertyName, object);
        if (!object.isAfter(clockProvider.get().instant())) {
            error(propertyName, "must be in the future");
        }
    }

    public void positiveOrZero(String propertyName, int object) {
        min(propertyName, object, 0);
    }

    public void min(String propertyName, int object, int minimumValue) {
        if (object < minimumValue) {
            error(propertyName, "must be greater than " + minimumValue);
        }
    }

    private void error(String propertyName, String message) {
        throw new IllegalArgumentException(
                (path.isEmpty() ? "" : path + ".") + propertyName + " " + message);
    }
}
