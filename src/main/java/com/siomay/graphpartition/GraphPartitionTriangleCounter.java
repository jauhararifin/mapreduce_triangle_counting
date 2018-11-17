package com.siomay.graphpartition;

import com.siomay.utils.EdgeInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Scanner;

public class GraphPartitionTriangleCounter {

    private Configuration conf = new Configuration();

    private String inputPath;
    private String outputPath;
    private FileSystem fileSystem;

    public GraphPartitionTriangleCounter(String inputPath, String outputPath) {
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
        Job job = new GraphPartitionJob(conf);
        job.setJobName("siomay.graphPartition.job1");

        EdgeInputFormat.addInputPath(job, new Path(this.inputPath));
        job.setInputFormatClass(EdgeInputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(this.outputPath + "/temp1"));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.waitForCompletion(true);
        if (!job.isSuccessful()) {
            System.out.println("Job 1 Failed");
            System.exit(-1);
        }
        System.out.println("Job 1 Completed");
    }

    private void runJob2() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new CombineGraphJob(conf);
        job.setJobName("siomay.graphPartition.job1");

        SequenceFileInputFormat.addInputPath(job, new Path(this.outputPath + "/temp1"));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(this.outputPath + "/temp2"));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.waitForCompletion(true);
        if (!job.isSuccessful()) {
            System.out.println("Job 2 Failed");
            System.exit(-1);
        }
        System.out.println("Job 2 Completed");
    }

    private void runFinalCounter() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new TriangleCounterJob(conf);
        job.setJobName("siomay.graphPartition.job3");

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

        GraphPartitionTriangleCounter app = new GraphPartitionTriangleCounter(args[0], args[1]);
        app.run();
    }

}
