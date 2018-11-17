package com.siomay.graphpartition;

import com.siomay.utils.LongPairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GraphPartitionJob extends Job {

    private static Long PARTITION_FACTOR = 20L;

    private static long hash(long p) {
        return (((p * 17 + 13) * p + 19) * p) % PARTITION_FACTOR;
    }

    public static class MapClass extends Mapper<LongWritable, LongPairWritable, LongWritable, LongPairWritable> {
        @Override
        protected void map(LongWritable key, LongPairWritable value, Context context) throws IOException, InterruptedException {
            long a = hash(value.getFirst());
            long b = hash(value.getSecond());
            for (long i = 0; i < PARTITION_FACTOR; i++) {
                for (long j = i + 1; j < PARTITION_FACTOR; j++) {
                    for (long k = j + 1; k < PARTITION_FACTOR; k++) {
                        if ((a == i || a == j || a == k) && (b == i || b == j || b == k)) {
                            long partitionId = i + PARTITION_FACTOR * j + PARTITION_FACTOR * PARTITION_FACTOR * k;
                            context.write(new LongWritable(partitionId), value);
                        }
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, LongPairWritable, LongWritable, LongWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongPairWritable> values, Context context) throws IOException, InterruptedException {
            Map<Long, Set<Long>> adjlist = new HashMap();

            for (LongPairWritable e : values) {
                long a = e.getFirst();
                long b = e.getSecond();
                if (a == b) {
                    continue;
                }
                if (a > b) {
                    long temp = a;
                    a = b;
                    b = temp;
                }

                if (!adjlist.containsKey(a)) {
                    adjlist.put(a, new HashSet<Long>());
                }
                adjlist.get(a).add(b);
            }

            for (Long a : adjlist.keySet()) {
                for (Long b : adjlist.get(a)) {
                    if (!adjlist.containsKey(b)) {
                        continue;
                    }
                    for (Long c : adjlist.get(b)) {
                        if (adjlist.get(a).contains(c)) {
                            long ha = hash(a);
                            long hb = hash(b);
                            long hc = hash(c);

                            long z = 1;
                            if (ha == hb && hb == hc) {
                                z = (ha * (ha - 1)) / 2
                                        + ha * (PARTITION_FACTOR - ha - 1)
                                        + ((PARTITION_FACTOR - ha - 1) * (PARTITION_FACTOR - ha - 2)) / 2;
                            } else if (ha == hb || ha == hc || hb == hc) {
                                z = PARTITION_FACTOR - 2;
                            }

                            context.write(new LongWritable(z), new LongWritable(1));
                        }
                    }
                }
            }
        }
    }

    private void setup() {
        setNumReduceTasks(1140);
        setMapperClass(MapClass.class);
        setMapOutputKeyClass(LongWritable.class);
        setMapOutputValueClass(LongPairWritable.class);
        setReducerClass(Reduce.class);
        setOutputKeyClass(LongWritable.class);
        setOutputValueClass(LongWritable.class);
    }

    public GraphPartitionJob() throws IOException {
        super();
        setup();
    }

    public GraphPartitionJob(Configuration conf) throws IOException {
        super(conf);
        setup();
    }

    public GraphPartitionJob(Configuration conf, String jobName) throws IOException {
        super(conf, jobName);
        setup();
    }

}
