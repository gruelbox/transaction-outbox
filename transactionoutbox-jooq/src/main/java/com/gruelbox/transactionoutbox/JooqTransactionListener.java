package com.gruelbox.transactionoutbox;

import lombok.extern.slf4j.Slf4j;

/** @deprecated use {@link com.gruelbox.transactionoutbox.jooq.JooqTransactionListener} */
@Slf4j
@Deprecated(forRemoval = true)
public class JooqTransactionListener extends com.gruelbox.transactionoutbox.jooq.JooqTransactionListener {

}
