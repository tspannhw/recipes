package com.immerok.cookbook.events;

import com.github.javafaker.Faker;
import java.util.Random;
import java.util.function.Supplier;

/** A supplier that produces Events. */
public class EventSupplier implements Supplier<Event> {
    private final Random random = new Random();
    private final Faker faker = new Faker();
    private int id = 0;

    @Override
    public Event get() {
        String ibanAccountNumber = faker.finance().iban();
        return new Event(
                id++,
                ibanAccountNumber,
                Event.Priority.values()[random.nextInt(Event.Priority.values().length)]);
    }
}
