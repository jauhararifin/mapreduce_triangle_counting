package com.siomay.neighborscounter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import static java.lang.Math.max;

public class NodeStatisticCounter extends Job {

    public static class Map extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {
        @Override
        protected void map(LongWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
            context.write(new LongWritable(0), new LongWritable(1));
            context.write(new LongWritable(1), value);
        }
    }

    public static class Combine extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) {
                long sum = 0;
                for (LongWritable val : values) {
                    sum += val.get();
                }
                context.write(key, new LongWritable(sum));
            }

            if (key.get() == 1) {
                long m = 0;
                for (LongWritable val : values) {
                    m = max(m, val.get());
                }
                context.write(key, new LongWritable(m));
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, LongWritable, Text, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            if (key.get() == 0) {
                long sum = 0;
                for (LongWritable val : values) {
                    sum += val.get();
                }
                context.write(new Text("number of node"), new Text(sum + ""));
            }

            if (key.get() == 1) {
                long m = 0;
                for (LongWritable val : values) {
                    m = max(m, val.get());
                }
                context.write(new Text("max neighbors"), new Text(m + ""));
            }
        }
    }

    private void setup() {
        setMapperClass(Map.class);
        setMapOutputKeyClass(LongWritable.class);
        setMapOutputValueClass(LongWritable.class);
        setReducerClass(Reduce.class);
        setCombinerClass(Combine.class);
        setOutputKeyClass(Text.class);
        setOutputValueClass(Text.class);
    }

    public NodeStatisticCounter() throws IOException {
        super();
        setup();
    }

    public NodeStatisticCounter(Configuration conf) throws IOException {
        super(conf);
        setup();
    }

    public NodeStatisticCounter(Configuration conf, String jobName) throws IOException {
        super(conf, jobName);
        setup();
    }

}
