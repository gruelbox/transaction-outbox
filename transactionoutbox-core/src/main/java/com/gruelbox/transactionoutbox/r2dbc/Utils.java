package com.gruelbox.transactionoutbox.r2dbc;

import io.r2dbc.spi.R2dbcTimeoutException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import java.util.function.BiFunction;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class Utils {

  private Utils() {}

  static final Result EMPTY_RESULT =
      new Result() {
        @Override
        public Publisher<Integer> getRowsUpdated() {
          return Mono.just(0);
        }

        @Override
        public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
          return Flux.empty();
        }
      };

  static final Result TIMEOUT_RESULT =
      new Result() {
        @Override
        public Publisher<Integer> getRowsUpdated() {
          throw new R2dbcTimeoutException();
        }

        @Override
        public <T> Publisher<T> map(BiFunction<Row, RowMetadata, ? extends T> mappingFunction) {
          throw new R2dbcTimeoutException();
        }
      };
}
