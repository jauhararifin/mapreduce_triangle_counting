package com.siomay;

import com.siomay.core.EdgeInputFormat;
import com.siomay.core.NodeIteratorCounterJob;
import com.siomay.core.NodeIteratorFirstJob;
import com.siomay.core.NodeIteratorSecondJob;
import com.sun.org.apache.xpath.internal.operations.Mult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Scanner;

public class TriangleCounter {

    private Configuration conf = new Configuration();

    private String inputPath;
    private String outputPath;
    private FileSystem fileSystem;

    public TriangleCounter(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    private FileSystem getFS() throws IOException {
        if (fileSystem == null) {
            fileSystem = FileSystem.get(conf);
        }
        return fileSystem;
    }

    private void cleanOutputDir() throws IOException {
        getFS().delete(new Path(this.outputPath), true);
    }

    private void runJob1() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new NodeIteratorFirstJob(conf);
        job.setJobName("siomay.nodeIteratorFirst");

        EdgeInputFormat.addInputPath(job, new Path(this.inputPath));
        job.setInputFormatClass(EdgeInputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(this.outputPath + "/temp"));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.waitForCompletion(true);
        if (!job.isSuccessful()) {
            System.out.println("Job 1 Failed");
            System.exit(-1);
        }
        System.out.println("Job 1 Completed");
    }

    private void runJob2() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new NodeIteratorSecondJob(conf);
        job.setJobName("siomay.nodeIteratorSecond");

        MultipleInputs.addInputPath(job, new Path(this.outputPath + "/temp"), SequenceFileInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(this.inputPath), EdgeInputFormat.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(this.outputPath + "/temp2"));

        job.waitForCompletion(true);
        if (!job.isSuccessful()) {
            System.out.println("Job 2 Failed");
            System.exit(-1);
        }
        System.out.println("Job 2 Completed");
    }

    private void runFinalCounter() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new NodeIteratorCounterJob(conf);
        job.setJobName("siomay.nodeIteratorCounter");

        SequenceFileInputFormat.addInputPath(job, new Path(this.outputPath + "/temp2"));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(this.outputPath + "/final"));
        job.setOutputFormatClass(TextOutputFormat.class);

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
        System.out.println("counted triangle: " + nTriangle);
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.printf("Needs two arguments, input and output files\n");
            System.exit(-1);
        }

        TriangleCounter app = new TriangleCounter(args[0], args[1]);
        app.run();
    }

}
