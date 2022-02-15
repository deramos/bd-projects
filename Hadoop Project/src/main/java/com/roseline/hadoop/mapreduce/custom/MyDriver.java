package com.roseline.hadoop.mapreduce.custom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class MyDriver {

    public static void main(String[] args) {
        try {
            if (args.length != 2) {
                System.out.println("Invalid number of arguments. Usage: <input path> <output path>");
                System.exit(-1);
            }

            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "Mapper with Custom InputFormat");

            job.setJarByClass(MyDriver.class);
            job.setMapperClass(MyMapper.class);
            job.setInputFormatClass(MyInputFormat.class);
            job.setNumReduceTasks(0);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);

        } catch (IOException e) {
            System.out.println(e);
        }
    }
}
