package com.siomay.core;

import com.siomay.utils.LongPairWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class Reducer2 extends Reducer<LongPairWritable, LongWritable, LongPairWritable, LongWritable> {

    @Override
    protected void reduce(LongPairWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<LongWritable> it = values.iterator();
        Long nElement = 0L;

        Set<Long> visited = new HashSet<Long>();
        boolean shouldCounted = false;
        while (it.hasNext()) {
            Long vertex = it.next().get();
            if (visited.contains(vertex)) {
                continue;
            }
            visited.add(vertex);

            if (vertex > -1) {
                nElement++;
            } else {
                shouldCounted = true;
            }
        }

        if (shouldCounted) {
            context.write(key, new LongWritable(nElement));
        }
    }
}
