package com.gruelbox.transactionoutbox;

import java.io.Reader;

public interface InvocationSerializer {

  String serialize(Invocation invocation);

  Invocation deserialize(Reader reader);
}
