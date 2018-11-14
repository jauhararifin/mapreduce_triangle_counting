package com.siomay.core;

import com.siomay.utils.LongPair;
import com.siomay.utils.LongPairArrayWritable;
import com.siomay.utils.LongPairWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper2 extends Mapper<LongWritable, LongPairArrayWritable, LongPairWritable, LongWritable> {

    @Override
    protected void map(LongWritable key, LongPairArrayWritable value, Context context) throws IOException, InterruptedException {
        LongPair[] inputArr = value.getLongPairs();
        for (LongPair pair : inputArr) {
            context.write(new LongPairWritable(pair), key);

            context.write(new LongPairWritable(
                    new LongPair(pair.getFirst(), key.get())
            ), new LongWritable(-1));

            context.write(new LongPairWritable(
                    new LongPair(key.get(), pair.getFirst())
            ), new LongWritable(-1));

            context.write(new LongPairWritable(
                    new LongPair(pair.getSecond(), key.get())
            ), new LongWritable(-1));

            context.write(new LongPairWritable(
                    new LongPair(key.get(), pair.getSecond())
            ), new LongWritable(-1));
        }
    }

}
