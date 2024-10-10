package com.gruelbox.transactionoutbox.spring;

import com.gruelbox.transactionoutbox.Dialect;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

@Component
public class StringToDialectConverter implements Converter<String, Dialect> {

    @Override
    public Dialect convert(String source) {
        try {
            Class<?> dialectClass = Class.forName(source);
            Constructor<?> constructor = dialectClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            return (Dialect) constructor.newInstance();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
