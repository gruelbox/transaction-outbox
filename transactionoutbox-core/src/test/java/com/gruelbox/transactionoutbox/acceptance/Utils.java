package com.gruelbox.transactionoutbox.acceptance;

import com.gruelbox.transactionoutbox.TransactionManager;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Statement;

class Utils {

  static String resourceAsString(String fileName) throws IOException {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    URL resource = Utils.class.getClassLoader().getResource(fileName);
    if (resource == null) {
      throw new IllegalArgumentException("Resource not found");
    }
    try (InputStream is = resource.openStream()) {
      is.transferTo(byteArrayOutputStream);
    }
    return new String(byteArrayOutputStream.toByteArray());
  }

  @SuppressWarnings("SameParameterValue")
  static void runSql(TransactionManager transactionManager, String sql) {
    transactionManager.inTransaction(
        () -> {
          try {
            try (Statement statement = transactionManager.getActiveConnection().createStatement()) {
              statement.execute(sql);
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }
}
