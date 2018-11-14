package com.siomay.core;

import com.siomay.utils.LongTriplet;
import com.siomay.utils.LongTripletWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Reducer2 extends Reducer<LongTripletWritable, LongWritable, LongTripletWritable, NullWritable> {

    @Override
    protected void reduce(LongTripletWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Set<Long> array = new HashSet<Long>();
        boolean shouldCounted = false;
        for (LongWritable val : values) {
            if (val.get() < 0) {
                shouldCounted = true;
            } else {
                array.add(val.get());
            }
        }

        if (shouldCounted) {
            for (Long val: array) {
                long a = key.get().getSecond();
                long b = key.get().getThird();
                long c = val;
                if (a > b) {
                    long temp = a;
                    a = b;
                    b = temp;
                }
                if (a > c) {
                    long temp = a;
                    a = c;
                    c = temp;
                }
                if (b > c) {
                    long temp = b;
                    b = c;
                    c = temp;
                }

                context.write(new LongTripletWritable(new LongTriplet(a,b,c)), NullWritable.get());
            }
        }
    }
}
