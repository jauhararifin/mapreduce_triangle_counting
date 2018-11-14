package com.siomay.utils;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LongTripletWritable implements WritableComparable<LongTripletWritable> {

    private LongTriplet value;

    public LongTripletWritable() {
    }

    public LongTripletWritable(LongTriplet value) {
        this.value = value;
    }

    public LongTriplet get() {
        return value;
    }

    public void set(LongTriplet value) {
        this.value = value;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(value.getFirst());
        dataOutput.writeLong(value.getSecond());
        dataOutput.writeLong(value.getThird());
    }

    public void readFields(DataInput dataInput) throws IOException {
        Long first = dataInput.readLong();
        Long second = dataInput.readLong();
        Long third = dataInput.readLong();
        value = new LongTriplet(first, second, third);
    }

    public int compareTo(LongTripletWritable longPairWritable) {
        return value.compareTo(longPairWritable.get());
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
