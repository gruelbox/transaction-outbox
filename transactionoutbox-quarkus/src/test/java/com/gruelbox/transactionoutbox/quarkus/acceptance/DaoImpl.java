package com.gruelbox.transactionoutbox.quarkus.acceptance;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

@ApplicationScoped
public class DaoImpl {
  @Inject DataSource defaultDataSource;

  @SuppressWarnings("UnusedReturnValue")
  public int writeSomethingIntoDatabase(String something) {
    if ("error".equals(something)) {
      throw new RuntimeException("Persistence error");
    }
    String insertQuery = "insert into toto values (?);";
    try (Connection connexion = defaultDataSource.getConnection();
        PreparedStatement statement = connexion.prepareStatement(insertQuery)) {
      statement.setString(1, something);
      return statement.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public List<String> getFromDatabase() {
    List<String> values = new ArrayList<>();
    try (Connection connexion = defaultDataSource.getConnection();
        Statement statement = connexion.createStatement()) {
      ResultSet resultSet = statement.executeQuery("select * from toto;");
      while (resultSet.next()) {
        values.add(resultSet.getString(1));
      }
      return values;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("UnusedReturnValue")
  public int purge() {
    try (Connection connexion = defaultDataSource.getConnection();
        Statement statement = connexion.createStatement()) {
      return statement.executeUpdate("delete from toto;");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
