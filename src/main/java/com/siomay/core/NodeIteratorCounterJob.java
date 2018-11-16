package com.siomay.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NodeIteratorCounterJob extends Job {

    public static class Map extends Mapper<NullWritable, LongWritable, NullWritable, LongWritable> {
        @Override
        protected void map(NullWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), value);
        }
    }

    public static class Reduce extends Reducer<NullWritable, LongWritable, NullWritable, LongWritable> {
        @Override
        protected void reduce(NullWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable temp : values) {
                sum += temp.get();
            }
            context.write(NullWritable.get(), new LongWritable(sum));
        }
    }

    private void setup() {
        setMapperClass(Map.class);
        setMapOutputKeyClass(NullWritable.class);
        setMapOutputValueClass(LongWritable.class);
        setCombinerClass(Reduce.class);
        setReducerClass(Reduce.class);
        setOutputKeyClass(NullWritable.class);
        setOutputValueClass(LongWritable.class);
    }

    public NodeIteratorCounterJob() throws IOException {
        super();
        setup();
    }

    public NodeIteratorCounterJob(Configuration conf) throws IOException {
        super(conf);
        setup();
    }

    public NodeIteratorCounterJob(Configuration conf, String jobName) throws IOException {
        super(conf, jobName);
        setup();
    }

}
