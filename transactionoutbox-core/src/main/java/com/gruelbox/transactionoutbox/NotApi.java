package com.gruelbox.transactionoutbox;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Signifies that a public class or method is not intended for client use. Such classes and methods
 * are not subject to any semver guarantees and may be broken without warning. It is strongly
 * advised not to use them in your code.
 */
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface NotApi {}
