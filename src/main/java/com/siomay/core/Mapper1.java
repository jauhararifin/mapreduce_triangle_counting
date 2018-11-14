package com.siomay.core;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Mapper1 extends Mapper<Object, Text, LongWritable, LongWritable> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String str = value.toString();
        if (str.contains("\t")) {
            StringTokenizer tokeniner = new StringTokenizer(str);
            String a = tokeniner.nextToken();
            Long idA = Long.parseLong(a);
            String b = tokeniner.nextToken();
            Long idB = Long.parseLong(b);

            if (idA != idB) {
                context.write(new LongWritable(idA), new LongWritable(idB));
                context.write(new LongWritable(idB), new LongWritable(idA));
            }
        }
    }

}
