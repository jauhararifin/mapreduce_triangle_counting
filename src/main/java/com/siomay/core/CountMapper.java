package com.siomay.core;

import com.siomay.utils.LongTripletWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CountMapper extends Mapper<LongTripletWritable, NullWritable, LongTripletWritable, NullWritable> {

    @Override
    protected void map(LongTripletWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
        context.write(key, value);
    }
}
