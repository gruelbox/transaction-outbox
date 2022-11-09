package com.synaos.transactionoutbox.acceptance;

import com.synaos.transactionoutbox.TransactionManager;

import java.sql.Statement;

public class TestUtils {

    @SuppressWarnings("SameParameterValue")
    public static void runSql(TransactionManager transactionManager, String sql) {
        transactionManager.inTransaction(
                tx -> {
                    try {
                        try (Statement statement = tx.connection().createStatement()) {
                            statement.execute(sql);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
    }
}
