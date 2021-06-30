package com.ontotext.trree.plugin.sequences;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a single sequence whose state is persisted across transactions.
 */
class Sequence {
    private final AtomicLong value;
    private transient volatile long committedValue;
    private transient volatile boolean prepared;

    Sequence() {
        this(0);
    }

    Sequence(long sequenceStart) {
        value = new AtomicLong(sequenceStart);
        committedValue = sequenceStart;
    }

    void prepare() {
        prepared = true;
    }

    void commit() {
        committedValue = value.get();
        prepared = false;
    }

    void rollback() {
        value.set(committedValue);
        prepared = false;
    }

    long nextValue() {
        return value.incrementAndGet();
    }

    long currentValue() {
        return value.get();
    }

    Sequence copy() {
        return new Sequence(value.get());
    }

    @JsonProperty
    public long getValue() {
        return value.get();
    }

    public void setValue(long value) {
        this.value.set(value);
    }

    @Override
    public int hashCode() {
        return prepared ? Objects.hash(value.get()) : Objects.hash(committedValue);
    }
}
