package com.siomay.core;

import com.siomay.utils.LongPair;
import com.siomay.utils.LongPairArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class Reducer1 extends Reducer<LongWritable, LongWritable, LongWritable, LongPairArrayWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        List<Long> valueArr = new ArrayList<Long>();
        for (LongWritable x : values) {
            valueArr.add(x.get());
        }

        List<LongPair> result = new ArrayList<LongPair>();
        Set<Long> visited = new HashSet<Long>();
        for (Long a : valueArr) {
            if (visited.contains(a)) {
                continue;
            }
            visited.add(a);

            Set<Long> visited2 = new HashSet<Long>();
            for (Long b : valueArr) {
                if (visited2.contains(b)) {
                    continue;
                }
                visited2.add(b);

                if (a != b) {
                    result.add(new LongPair(a,b));
                }
            }
        }

        LongPair[] resultArray = new LongPair[result.size()];
        LongPairArrayWritable emittedVal = new LongPairArrayWritable();
        emittedVal.set(result.toArray(resultArray));
        context.write(key, emittedVal);
    }

}
