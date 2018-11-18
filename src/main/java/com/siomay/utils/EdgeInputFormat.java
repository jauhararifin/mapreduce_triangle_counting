package com.siomay.utils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.util.StringTokenizer;

public class EdgeInputFormat extends FileInputFormat<LongWritable, LongPairWritable> {

    @Override
    public RecordReader<LongWritable, LongPairWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new EdgeRecordReader();
    }

    private static class EdgeRecordReader extends RecordReader<LongWritable, LongPairWritable> {

        private LineRecordReader lineRecordReader;
        private Long key;
        private LongPair value;

        public EdgeRecordReader() {
            this.lineRecordReader = new LineRecordReader();
        }

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException {
            lineRecordReader.initialize(inputSplit, taskAttemptContext);
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            boolean result = lineRecordReader.nextKeyValue();
            while (result) {
                Text text = lineRecordReader.getCurrentValue();
                String str = text.toString();
                if (str.contains("\t")) {
                    StringTokenizer tokeniner = new StringTokenizer(str);
                    Long idA = 0L;
                    Long idB = 0L;
                    try {
                        idA = Long.parseLong(tokeniner.nextToken());
                        idB = Long.parseLong(tokeniner.nextToken());
                    } catch (Exception e) {
                        result = lineRecordReader.nextKeyValue();
                        continue;
                    }
                    value = new LongPair(idA, idB);
                    break;
                } else {
                    result = lineRecordReader.nextKeyValue();
                }
            }
            return result;
        }

        @Override
        public LongWritable getCurrentKey() {
            return new LongWritable(-1);
        }

        @Override
        public LongPairWritable getCurrentValue() {
            return new LongPairWritable(value);
        }

        @Override
        public float getProgress() throws IOException {
            return lineRecordReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            lineRecordReader.close();
        }


    }

}
