package com.roseline.hadoop.mapreduce.sum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class SumDriver {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

            Job job = Job.getInstance(conf, "Total Spend By Products with Combiner");

            job.setJarByClass(SumDriver.class);
            job.setMapperClass(SumMapper.class);
            job.setReducerClass(SumReducer.class);
            job.setCombinerClass(SumCombiner.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (IOException | InterruptedException | ClassNotFoundException exception) {
            System.out.println(exception.getMessage());
        }
    }
}
