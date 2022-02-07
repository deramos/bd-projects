package com.roseline.hadoop.mapreduce.sum;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SumMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String transactionString = value.toString();
        String[] transactionData = transactionString.split(",");
        double amount = Double.parseDouble(transactionData[3]);
        context.write(new Text(transactionData[5].trim()), new DoubleWritable(amount));
    }

}
