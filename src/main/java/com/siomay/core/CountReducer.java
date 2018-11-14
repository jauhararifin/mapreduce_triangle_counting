package com.siomay.core;

import com.siomay.utils.LongPairWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigInteger;

public class CountReducer extends Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {

    @Override
    protected void reduce(NullWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        Long sum = 0L;
        for (LongWritable val : values) {
            Long num = val.get();
            sum += num;
        }
        context.write(NullWritable.get(), new LongWritable(sum));
    }

}
