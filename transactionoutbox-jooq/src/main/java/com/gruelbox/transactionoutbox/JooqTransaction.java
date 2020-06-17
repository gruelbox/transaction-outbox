package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.SimpleTransaction;
import java.sql.Connection;
import org.jooq.Configuration;

public class JooqTransaction extends SimpleTransaction<Configuration> {
  JooqTransaction(Connection connection, Configuration configuration) {
    super(connection, configuration);
  }
}
