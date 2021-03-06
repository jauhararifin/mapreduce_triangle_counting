package com.siomay.utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongPairWritable implements WritableComparable<LongPairWritable> {

    private LongPair value;

    public LongPairWritable() {
    }

    public LongPairWritable(Long first, Long second) {
        value = new LongPair(first, second);
    }

    public LongPairWritable(LongPair value) {
        this.value = value;
    }

    public LongPair get() {
        return value;
    }

    public void set(LongPair value) {
        this.value = value;
    }

    public Long getFirst() {
        return value.getFirst();
    }

    public void setFirst(Long first) {
        value.setFirst(first);
    }

    public Long getSecond() {
        return value.getSecond();
    }

    public void setSecond(Long second) {
        value.setSecond(second);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(value.getFirst());
        dataOutput.writeLong(value.getSecond());
    }

    public void readFields(DataInput dataInput) throws IOException {
        Long first = dataInput.readLong();
        Long second = dataInput.readLong();
        value = new LongPair(first, second);
    }

    public int compareTo(LongPairWritable longPairWritable) {
        return value.compareTo(longPairWritable.get());
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
