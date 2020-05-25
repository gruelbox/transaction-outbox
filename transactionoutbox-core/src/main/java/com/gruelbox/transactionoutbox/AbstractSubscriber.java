package com.gruelbox.transactionoutbox;

import java.util.concurrent.CompletableFuture;
import lombok.AllArgsConstructor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

@AllArgsConstructor
class AbstractSubscriber<T> implements Subscriber<T> {

  private final CompletableFuture<?> future;

  @Override
  public void onSubscribe(Subscription s) {}

  @Override
  public void onComplete() {}

  @Override
  public void onNext(T result) {}

  @Override
  public void onError(Throwable t) {
    future.completeExceptionally(t);
  }
}
