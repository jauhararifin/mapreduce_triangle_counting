package com.siomay.core;

import com.siomay.utils.LongPairArrayWritable;
import com.siomay.utils.LongPairWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Scanner;

public class Application {

    private String inputPath;
    private String outputPath;
    private FileSystem fileSystem;

    public Application(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    private FileSystem getFS() throws IOException {
        if (fileSystem == null) {
            fileSystem = FileSystem.get(new Configuration());
        }
        return fileSystem;
    }

    private void cleanOutputDir() throws IOException {
        getFS().delete(new Path(this.outputPath), true);
    }

    private void runJob1() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job();
        job.setJarByClass(Application.class);
        job.setJobName("siomay.job1");

        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(this.inputPath));
        job.setMapperClass(Mapper1.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(this.outputPath + "/temp"));
        job.setReducerClass(Reducer1.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongPairArrayWritable.class);

        job.waitForCompletion(true);
        if (!job.isSuccessful()) {
            System.out.println("Job 1 Failed");
            System.exit(-1);
        }
        System.out.println("Job 1 Completed");
    }

    private void runJob2() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job();
        job.setJarByClass(Application.class);
        job.setJobName("siomay.job2");

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(this.outputPath + "/temp"));
        job.setMapperClass(Mapper2.class);
        job.setMapOutputKeyClass(LongPairWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(this.outputPath + "/temp2"));
        job.setReducerClass(Reducer2.class);
        job.setOutputKeyClass(LongPairWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.waitForCompletion(true);
        if (!job.isSuccessful()) {
            System.out.println("Job 2 Failed");
            System.exit(-1);
        }
        System.out.println("Job 2 Completed");
    }

    private void runFinalCounter() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job();
        job.setJarByClass(Application.class);
        job.setJobName("siomay.finalCounter");

        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(this.outputPath + "/temp2"));
        job.setMapperClass(CountMapper.class);
        job.setCombinerClass(CountReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(this.outputPath + "/final"));
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.waitForCompletion(true);
        if (!job.isSuccessful()) {
            System.out.println("Final Job Counter Failed");
            System.exit(-1);
        }
        System.out.println("Final Job Counter Completed");
    }

    public void run() throws IOException {
        try {
            this.cleanOutputDir();
        } catch (IOException e) {
            System.out.println("Caught error when cleaning output directory");
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            runJob1();
        } catch (Exception e) {
            System.out.println("Caught error when running job 1");
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            runJob2();
        } catch (Exception e) {
            System.out.println("Caught error when running job 1");
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            runFinalCounter();
        } catch (Exception e) {
            System.out.println("Caught error when running final counter job");
            e.printStackTrace();
            System.exit(-1);
        }

        System.out.println("All Job Completed");

        FSDataInputStream is = getFS().open(new Path(this.outputPath + "/final/part-r-00000"));
        Scanner scanner = new Scanner(is);
        String output = scanner.next();
        Long nTriangle = Long.parseLong(output);

        FSDataOutputStream os = getFS().create(new Path(this.outputPath + "/result.txt"));
        os.writeChars("counted triangle: " + nTriangle + "\n");
        os.writeChars("real triangle: " + (nTriangle / 6) + "\n");

        System.out.println("counted triangle: " + nTriangle);
        System.out.println("real triangle: " + (nTriangle / 6));
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.printf("Needs two arguments, input and output files\n");
            System.exit(-1);
        }

        Application app = new Application(args[0], args[1]);
        app.run();
    }

}
