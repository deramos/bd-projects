package com.roseline.hadoop.mapreduce.sum;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SumCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0.0;
        for (DoubleWritable value : values) {
            sum += value.get();
        }
        context.write(key, new DoubleWritable(sum));
    }

}
