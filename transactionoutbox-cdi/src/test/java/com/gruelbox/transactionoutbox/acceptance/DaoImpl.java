package com.gruelbox.transactionoutbox.acceptance;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.sql.DataSource;


@ApplicationScoped
public class DaoImpl
{
   @Inject
   DataSource defaultDataSource;

   public int writeSomethingIntoDatabase(String something)
   {
      if ("error".equals(something))
      {
         throw new RuntimeException("Persistence error");
      }
      String insertQuery = "insert into toto values (?);";
      try (Connection connexion = defaultDataSource.getConnection(); PreparedStatement statement = connexion.prepareStatement(insertQuery);)
      {
         statement.setString(1, something);
         return statement.executeUpdate();
      }
      catch (SQLException e)
      {
         throw new RuntimeException(e);
      }
   }

   public List<String> getFromDatabase()
   {
      List<String> values = new ArrayList<>();
      try (Connection connexion = defaultDataSource.getConnection(); Statement statement = connexion.createStatement();)
      {
         ResultSet resultSet = statement.executeQuery("select * from toto;");
         while (resultSet.next())
         {
            values.add(resultSet.getString(1));
         }
         return values;
      }
      catch (SQLException e)
      {
         throw new RuntimeException(e);
      }
   }

   public int purge()
   {
      try (Connection connexion = defaultDataSource.getConnection(); Statement statement = connexion.createStatement();)
      {
         return statement.executeUpdate("delete from toto;");
      }
      catch (SQLException e)
      {
         throw new RuntimeException(e);
      }
   }
}
