package com.siomay.core;

import com.siomay.utils.LongPairWritable;
import com.siomay.utils.LongTriplet;
import com.siomay.utils.LongTripletWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Reducer2 extends Reducer<LongPairWritable, LongWritable, LongTripletWritable, NullWritable> {

    @Override
    protected void reduce(LongPairWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
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
                Long a = key.get().getFirst();
                Long b = key.get().getSecond();
                Long c = val;
                if (a > b) {
                    Long temp = a;
                    a = b;
                    b = temp;
                }
                if (a > c) {
                    Long temp = a;
                    a = c;
                    c = temp;
                }
                if (b > c) {
                    Long temp = b;
                    b = c;
                    c = temp;
                }

                context.write(new LongTripletWritable(new LongTriplet(a,b,c)), NullWritable.get());
            }
        }
    }
}
