package com.roseline.hadoop.mapreduce.sum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AgePartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text text, Text text2, int numPartitions) {
        return 0;
    }
}
