package com.immerok.cookbook.records;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Iterator;
import java.util.Random;

/** An Iterator that produces a stream of duplicated Transactions. */
public class TransactionIterator implements Iterator<Transaction> {
    private static final int TOTAL_CUSTOMERS = 2;

    private static final Random random = new Random();
    private int id = 0;

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Transaction next() {
        return new Transaction(
                Instant.now(),
                this.id++,
                random.nextInt(TOTAL_CUSTOMERS),
                new BigDecimal(1000.0 * random.nextFloat()));
    }
}
