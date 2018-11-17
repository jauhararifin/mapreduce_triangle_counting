package com.siomay.graphpartition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CombineGraphJob extends Job {

    public static class MapClass extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {
        @Override
        protected void map(LongWritable key, LongWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class Combine extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    public static class Reduce extends Reducer<LongWritable, LongWritable, NullWritable, LongWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            context.write(NullWritable.get(), new LongWritable(sum / key.get()));
        }
    }

    private void setup() {
        setNumReduceTasks(1);
        setMapperClass(MapClass.class);
        setMapOutputKeyClass(LongWritable.class);
        setMapOutputValueClass(LongWritable.class);
        setReducerClass(Reduce.class);
        setCombinerClass(Combine.class);
        setOutputKeyClass(NullWritable.class);
        setOutputValueClass(LongWritable.class);
    }

    public CombineGraphJob() throws IOException {
        super();
        setup();
    }

    public CombineGraphJob(Configuration conf) throws IOException {
        super(conf);
        setup();
    }

    public CombineGraphJob(Configuration conf, String jobName) throws IOException {
        super(conf, jobName);
        setup();
    }

}
