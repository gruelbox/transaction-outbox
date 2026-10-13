package com.gruelbox.transactionoutbox.jackson3;

public enum Color {
  RED,
  GREEN;

  @Override
  public String toString() {
    return name().toLowerCase();
  }
}
