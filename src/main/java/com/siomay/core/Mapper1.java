package com.siomay.core;

import com.siomay.utils.LongPair;
import com.siomay.utils.LongPairWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Mapper1 extends Mapper<Object, Text, LongPairWritable, LongWritable> {

    private int PARTITION_FACTOR = 10;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String str = value.toString();
        if (str.contains("\t")) {
            StringTokenizer tokeniner = new StringTokenizer(str);
            Long idA = Long.parseLong(tokeniner.nextToken());
            Long idB = Long.parseLong(tokeniner.nextToken());

            if (idA.equals(idB)) {
                return;
            }

            for (int a = 0; a < PARTITION_FACTOR; a++) {
                for (int b = a + 1; b < PARTITION_FACTOR; b++) {
                    for (int c = b + 1; c < PARTITION_FACTOR; c++) {
                        int x = (int) (idA % (long) PARTITION_FACTOR);
                        int y = (int) (idB % (long) PARTITION_FACTOR);
                        if ((x == a || x == b || x == c) && (y == a || y == b || y == c)) {
                            long graphId = a + b * PARTITION_FACTOR + c * PARTITION_FACTOR * PARTITION_FACTOR;
                            context.write(new LongPairWritable(new LongPair(graphId, idA)), new LongWritable(idB));
                            context.write(new LongPairWritable(new LongPair(graphId, idB)), new LongWritable(idA));
                        }
                    }
                }
            }
        }
    }

}
