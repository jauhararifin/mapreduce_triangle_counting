package com.siomay.utils;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class LongPairArrayWritable extends ArrayWritable {

    public LongPairArrayWritable() {
        super(LongPairWritable.class);
    }

    public void set(LongPair[] values) {
        LongPairWritable[] array = new LongPairWritable[values.length];
        for (int i = 0; i < values.length; i++) {
            array[i] = new LongPairWritable(values[i]);
        }
        super.set(array);
    }

    public LongPair[] getLongPairs() {
        Writable[] array = super.get();
        LongPair[] result = new LongPair[array.length];
        for (int i = 0; i < array.length; i++) {
            result[i] = ((LongPairWritable) array[i]).get();
        }
        return result;
    }
}
