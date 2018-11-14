package com.siomay.core;

import com.siomay.utils.LongPair;
import com.siomay.utils.LongPairWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Reducer1 extends Reducer<LongWritable, LongWritable, LongWritable, LongPairWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Set<Long> valueArr = new HashSet<Long>();
        for (LongWritable x : values) {
            valueArr.add(x.get());
        }

        for (Long a : valueArr) {
            for (Long b : valueArr) {
                if (a != b) {
                    context.write(key, new LongPairWritable(new LongPair(a, b)));
                }
            }
        }

        valueArr.clear();
    }

}
