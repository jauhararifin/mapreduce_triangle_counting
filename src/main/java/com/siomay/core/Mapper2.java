package com.siomay.core;

import com.siomay.utils.LongPairWritable;
import com.siomay.utils.LongTriplet;
import com.siomay.utils.LongTripletWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper2 extends Mapper<LongPairWritable, LongPairWritable, LongTripletWritable, LongWritable> {

    @Override
    protected void map(LongPairWritable key, LongPairWritable value, Context context) throws IOException, InterruptedException {
        context.write(
                new LongTripletWritable(
                        new LongTriplet(
                                key.get().getFirst(),
                                value.get().getFirst(),
                                value.get().getSecond()
                        )
                ), new LongWritable(key.get().getSecond())
        );

        context.write(new LongTripletWritable(
                new LongTriplet(key.get().getFirst(), value.get().getFirst(), key.get().getSecond())
        ), new LongWritable(-1));

        context.write(new LongTripletWritable(
                new LongTriplet(key.get().getFirst(), key.get().getSecond(), value.get().getFirst())
        ), new LongWritable(-1));

        context.write(new LongTripletWritable(
                new LongTriplet(key.get().getFirst(), value.get().getSecond(), key.get().getSecond())
        ), new LongWritable(-1));

        context.write(new LongTripletWritable(
                new LongTriplet(key.get().getFirst(), key.get().getSecond(), value.get().getSecond())
        ), new LongWritable(-1));
    }

}
