package com.siomay.nodeiterator;

import com.siomay.utils.LongPairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static java.lang.Math.max;
import static java.lang.Math.min;

public class NodeIteratorFirstJob extends Job {

    public static class Map extends Mapper<LongWritable, LongPairWritable, LongWritable, LongWritable> {
        @Override
        protected void map(LongWritable key, LongPairWritable value, Context context) throws IOException, InterruptedException {
            long a = value.getFirst();
            long b = value.getSecond();
            if (a != b) {
                context.write(new LongWritable(min(a,b)), new LongWritable(max(a,b)));
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, LongWritable, LongWritable, LongPairWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            Set<Long> vertices = new HashSet<>();
            for (LongWritable val : values) {
                vertices.add(val.get());
            }

            for (Long a : vertices) {
                for (Long b : vertices) {
                    if (!a.equals(b) && a < b) {
                        context.write(key, new LongPairWritable(a, b));
                    }
                }
            }

            vertices.clear();
            System.gc();
        }
    }

    private void setup() {
        setMapperClass(Map.class);
        setNumReduceTasks(20);
        setMapOutputKeyClass(LongWritable.class);
        setMapOutputValueClass(LongWritable.class);
        setReducerClass(Reduce.class);
        setOutputKeyClass(LongWritable.class);
        setOutputValueClass(LongPairWritable.class);
    }

    public NodeIteratorFirstJob() throws IOException {
        super();
        setup();
    }

    public NodeIteratorFirstJob(Configuration conf) throws IOException {
        super(conf);
        setup();
    }

    public NodeIteratorFirstJob(Configuration conf, String jobName) throws IOException {
        super(conf, jobName);
        setup();
    }

}
