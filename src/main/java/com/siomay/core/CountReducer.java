package com.siomay.core;

import com.siomay.utils.LongTripletWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountReducer extends Reducer<LongTripletWritable, LongWritable, NullWritable, LongWritable> {

    private Long nTriangle = 0L;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        nTriangle = 0L;
    }

    @Override
    protected void reduce(LongTripletWritable key, Iterable<LongWritable> values, Context context) {
        nTriangle++;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), new LongWritable(nTriangle));
    }
}
