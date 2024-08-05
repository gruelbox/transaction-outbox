package com.gruelbox.transactionoutbox;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class DialectTest {
  @Test
  void testWhenDialectNameDoesNotExist_shouldThrowException() {
    // Given
    String dialectName = "bad dialect";

    // When
    Exception result =
        assertThrows(RuntimeException.class, () -> Dialect.getValueByName(dialectName));

    // Then
    assertTrue(result.getMessage().startsWith("Unknown dialect: " + dialectName));
    System.out.println(result.getMessage());
  }

  @Test
  void testWhenDialectNameDoesExistsButInvalidCase_shouldThrowException() {
    // Given
    String dialectName = "h2";

    // When
    Exception result =
        assertThrows(RuntimeException.class, () -> Dialect.getValueByName(dialectName));

    // Then
    assertTrue(result.getMessage().startsWith("Unknown dialect: " + dialectName));
  }

  @Test
  void testWhenDialectNameDoesExist_shouldReturnDialect() {
    // Given
    String dialectName = "H2";

    // When
    Dialect result = Dialect.getValueByName(dialectName);

    // Then
    assertEquals(Dialect.H2, result);
  }
}
