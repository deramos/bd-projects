package com.roseline.hadoop.mapreduce.sum;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;

public class SumReducer extends Reducer<Text, DoubleWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        Double sum = 0.0;
        for (DoubleWritable value : values) {
            sum += value.get();
        }
        DecimalFormat formatter = new DecimalFormat("0.##");
        context.write(key, new Text(formatter.format(sum)));
    }
}
