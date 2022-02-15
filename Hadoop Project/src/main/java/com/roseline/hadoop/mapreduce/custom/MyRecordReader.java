package com.roseline.hadoop.mapreduce.custom;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class MyRecordReader extends RecordReader <SensorKey, SensorValue> {
    private SensorKey key;
    private SensorValue value;
    private final LineRecordReader recordReader = new LineRecordReader();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        recordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean hasNextKeyValue = recordReader.nextKeyValue();

        if (hasNextKeyValue) {
            if (key == null) { key = new SensorKey(); }
            if (value == null) { value = new SensorValue(); }

            Text line = recordReader.getCurrentValue();
            String[] tokens = line.toString().split("\t");
            key.setType(new Text(tokens[0]));
            key.setEquip_id(new Text(tokens[1]));
            key.setLocation(new Text(tokens[2]));

            value.setDate(new Text(tokens[3]));
            value.setReading(new Text(tokens[4]));

        } else {
            key = null;
            value = null;
        }

        return false;
    }

    @Override
    public SensorKey getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public SensorValue getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return recordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        recordReader.close();
    }
}
