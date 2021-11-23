package com.gruelbox.transactionoutbox;

public class MissingOptionalDependencyException extends RuntimeException {

  public MissingOptionalDependencyException(String groupId, String artifactId) {
    super(
        String.format(
            "You are trying to use an optional feature, which requires an additional dependency (%s:%s). Please add it to your classpath, and try again.",
            groupId, artifactId));
  }
}
