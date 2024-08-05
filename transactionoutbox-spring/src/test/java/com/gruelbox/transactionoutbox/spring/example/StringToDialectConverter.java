package com.gruelbox.transactionoutbox.spring.example;

import com.gruelbox.transactionoutbox.Dialect;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

@Component
public class StringToDialectConverter implements Converter<String, Dialect> {

  @Override
  public Dialect convert(String source) {
    return Dialect.getValueByName(source);
  }
}
