package com.gruelbox.transactionoutbox;

import com.gruelbox.transactionoutbox.jdbc.StubParameterContextJdbcTransactionManager;
import java.sql.Connection;
import org.jooq.Configuration;
import org.jooq.impl.DefaultConfiguration;

public class StubDefaultJooqTransactionManager
    extends StubParameterContextJdbcTransactionManager<Configuration, JooqTransaction>
    implements DefaultJooqTransactionManager {
  public StubDefaultJooqTransactionManager() {
    super(
        Configuration.class,
        DefaultConfiguration::new,
        cfg -> new JooqTransaction(Utils.createLoggingProxy(Connection.class), cfg));
  }
}
