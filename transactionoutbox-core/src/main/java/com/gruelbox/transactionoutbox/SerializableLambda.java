package com.gruelbox.transactionoutbox;

import static java.util.stream.Collectors.joining;

import com.gruelbox.transactionoutbox.spi.Utils;
import java.io.Serializable;
import java.util.Arrays;

/** TODO */
@FunctionalInterface
public interface SerializableLambda extends Serializable {

  void run(LambdaContext lambdaContext);

  default String getDescription() {
    var className = getClass().getName();
    var index = className.indexOf("$$Lambda/");
    if (index == -1) {
      return "<invalid lambda>";
    }
    var parentClass = className.substring(0, index);
    var lambdaId = className.substring(index + 9);
    var args =
        Arrays.stream(getClass().getDeclaredFields())
            .map(
                f -> {
                  try {
                    f.setAccessible(true);
                    return f.get(this);
                  } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                  }
                })
            .map(Utils::stringifyArg)
            .collect(joining(","));
    return String.format("%s.lambda_%s_(%s)", parentClass, lambdaId, args);
  }
}
