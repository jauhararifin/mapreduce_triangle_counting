package com.siomay.core;

import com.siomay.utils.LongPair;
import com.siomay.utils.LongPairWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper2 extends Mapper<LongWritable, LongPairWritable, LongPairWritable, LongWritable> {

    @Override
    protected void map(LongWritable key, LongPairWritable value, Context context) throws IOException, InterruptedException {
        context.write(value, key);

        context.write(new LongPairWritable(
                new LongPair(value.get().getFirst(), key.get())
        ), new LongWritable(-1));

        context.write(new LongPairWritable(
                new LongPair(key.get(), value.get().getFirst())
        ), new LongWritable(-1));

        context.write(new LongPairWritable(
                new LongPair(value.get().getSecond(), key.get())
        ), new LongWritable(-1));

        context.write(new LongPairWritable(
                new LongPair(key.get(), value.get().getSecond())
        ), new LongWritable(-1));
    }

}
