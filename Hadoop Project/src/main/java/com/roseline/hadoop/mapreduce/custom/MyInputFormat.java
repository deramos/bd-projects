package com.roseline.hadoop.mapreduce.custom;

import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;
import java.util.List;

public class MyInputFormat extends InputFormat<SensorKey, SensorValue> {

    @Override
    public RecordReader<SensorKey, SensorValue> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new MyRecordReader();
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        return null;
    }
}
