package com.gruelbox.transactionoutbox;


@NotApi
public interface SchedulerProxyFactory {

  /**
   * Returns a proxy of {@code T} which, when called, will instantly return and schedule a call of
   * the <em>real</em> method to occur at some implementation-specific point.
   *
   * @param clazz The class to proxy.
   * @param <T> The type to proxy.
   * @return The proxy of {@code X}.
   */
  <T> T schedule(Class<T> clazz);
}
