package com.immerok.cookbook.records;

import com.github.javafaker.Faker;
import java.util.function.Supplier;

/** A supplier that produces Customers. */
public class CustomerSupplier implements Supplier<Customer> {
    public static final int TOTAL_CUSTOMERS = 6;
    private int id = 0;
    private final Faker faker = new Faker();

    @Override
    public Customer get() {
        String name = faker.name().firstName() + " " + faker.name().lastName();
        return new Customer(id++, name);
    }
}
