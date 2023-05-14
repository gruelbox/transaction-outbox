package com.gruelbox.transactionoutbox.r2dbc;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.NonNull;

class Utils {

  private Utils() {}

  static final Result EMPTY_RESULT =
      new Result() {
        @Override
        @NonNull
        public Publisher<Long> getRowsUpdated() {
          return Mono.just(0L);
        }

        @Override
        @NonNull
        public <T> Publisher<T> map(
            @NonNull BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
          return Flux.empty();
        }

        @Override
        @NonNull
        public Result filter(@NonNull Predicate<Segment> predicate) {
          return this;
        }

        @Override
        @NonNull
        public <T> Publisher<T> flatMap(
            @NonNull Function<Segment, ? extends Publisher<? extends T>> function) {
          return Flux.empty();
        }
      };
}
