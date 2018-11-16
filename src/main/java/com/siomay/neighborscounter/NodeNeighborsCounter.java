package com.siomay.neighborscounter;

import com.siomay.utils.LongPairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NodeNeighborsCounter extends Job {

    public static class Map extends Mapper<LongWritable, LongPairWritable, LongWritable, LongWritable> {
        @Override
        protected void map(LongWritable key, LongPairWritable value, Context context) throws IOException, InterruptedException {
            long a = value.getFirst();
            long b = value.getSecond();
            if (a != b) {
                context.write(new LongWritable(a), new LongWritable(1));
                context.write(new LongWritable(b), new LongWritable(1));
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable x : values) {
                sum += x.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    private void setup() {
        setMapperClass(Map.class);
        setMapOutputKeyClass(LongWritable.class);
        setMapOutputValueClass(LongWritable.class);
        setReducerClass(Reduce.class);
        setCombinerClass(Reduce.class);
        setOutputKeyClass(LongWritable.class);
        setOutputValueClass(LongWritable.class);
    }

    public NodeNeighborsCounter() throws IOException {
        super();
        setup();
    }

    public NodeNeighborsCounter(Configuration conf) throws IOException {
        super(conf);
        setup();
    }

    public NodeNeighborsCounter(Configuration conf, String jobName) throws IOException {
        super(conf, jobName);
        setup();
    }

}
