package com.siomay.nodeiterator;

import com.siomay.utils.LongPairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class NodeIteratorSecondJob extends Job {

    public static class Map extends Mapper<LongWritable, LongPairWritable, LongPairWritable, LongWritable> {
        @Override
        protected void map(LongWritable key, LongPairWritable value, Context context) throws IOException, InterruptedException {
            if (!value.getFirst().equals(value.getSecond())) {
                long a = value.getFirst();
                long b = value.getSecond();
                context.write(new LongPairWritable(min(a,b), max(a,b)), key);
            }
        }
    }

    public static class Reduce extends Reducer<LongPairWritable, LongWritable, NullWritable, LongWritable> {
        @Override
        protected void reduce(LongPairWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            boolean connected = false;
            Set<Long> visited = new HashSet<>();
            for (LongWritable val : values) {
                long v = val.get();
                visited.add(v);
                if (v == -1) {
                    connected = true;
                }
            }

            if (connected) {
                context.write(NullWritable.get(), new LongWritable(visited.size() - 1));
            }

            visited.clear();
            System.gc();
        }
    }

    private void setup() {
        setMapperClass(Map.class);
        setMapOutputKeyClass(LongPairWritable.class);
        setMapOutputValueClass(LongWritable.class);
        setReducerClass(Reduce.class);
        setOutputKeyClass(NullWritable.class);
        setOutputValueClass(LongWritable.class);
    }

    public NodeIteratorSecondJob() throws IOException {
        super();
        setup();
    }

    public NodeIteratorSecondJob(Configuration conf) throws IOException {
        super(conf);
        setup();
    }

    public NodeIteratorSecondJob(Configuration conf, String jobName) throws IOException {
        super(conf, jobName);
        setup();
    }

}
