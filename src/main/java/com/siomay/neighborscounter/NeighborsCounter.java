package com.siomay.neighborscounter;

import com.siomay.utils.EdgeInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Scanner;

public class NeighborsCounter {

    private Configuration conf = new Configuration();

    private String inputPath;
    private String outputPath;
    private FileSystem fileSystem;

    public NeighborsCounter(String inputPath, String outputPath) {
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

    private void runNeighborsCounter() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new NodeNeighborsCounter(conf);
        job.setJarByClass(NeighborsCounter.class);
        job.setJobName("siomay.countNeighbors");

        EdgeInputFormat.addInputPath(job, new Path(this.inputPath));
        job.setInputFormatClass(EdgeInputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path(this.outputPath + "/temp"));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.waitForCompletion(true);
        if (!job.isSuccessful()) {
            System.out.println("Job Failed");
            System.exit(-1);
        }
        System.out.println("Job Completed");
    }

    private void runStatisticNeighborsCounter() throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new NodeStatisticCounter(conf);
        job.setJarByClass(NeighborsCounter.class);
        job.setJobName("siomay.statisticNode");

        SequenceFileInputFormat.addInputPath(job, new Path(this.outputPath + "/temp"));
        job.setInputFormatClass(SequenceFileInputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(this.outputPath + "/final"));
        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);
        if (!job.isSuccessful()) {
            System.out.println("Job Failed");
            System.exit(-1);
        }
        System.out.println("Job Completed");
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
            runNeighborsCounter();
        } catch (Exception e) {
            System.out.println("Caught error when running job 1");
            e.printStackTrace();
            System.exit(-1);
        }

        try {
            runStatisticNeighborsCounter();
        } catch (Exception e) {
            System.out.println("Caught error when running job 1");
            e.printStackTrace();
            System.exit(-1);
        }

        FSDataInputStream is = getFS().open(new Path(this.outputPath + "/final/part-r-00000"));
        Scanner scanner = new Scanner(is);
        while (scanner.hasNext()) {
            String output = scanner.nextLine();
            System.out.println(output);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.printf("Needs two arguments, input and output files\n");
            System.exit(-1);
        }

        NeighborsCounter app = new NeighborsCounter(args[0], args[1]);
        app.run();
    }

}
