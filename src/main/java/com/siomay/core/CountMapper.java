package com.siomay.core;

import com.siomay.utils.LongPairWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountMapper extends Mapper<LongPairWritable, LongWritable, NullWritable, LongWritable> {

    @Override
    protected void map(LongPairWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), value);
    }
}
