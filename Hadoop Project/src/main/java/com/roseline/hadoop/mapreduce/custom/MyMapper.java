package com.roseline.hadoop.mapreduce.custom;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<SensorKey, SensorValue, Text, Text> {

    @Override
    protected void map(SensorKey key, SensorValue value, Context context) throws IOException, InterruptedException {
        String sensor = key.getType().toString();

        if (sensor.equalsIgnoreCase("Sys-M")) {
            context.write(value.getDate(), value.getReading());
        }
    }
}
