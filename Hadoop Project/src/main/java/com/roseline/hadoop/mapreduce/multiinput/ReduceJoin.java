package com.roseline.hadoop.mapreduce.multiinput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReduceJoin {

    public static class CustomerMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String record = value.toString();
            String[] parts = record.split(",");
            context.write(new Text(parts[0]), new Text("custs\t" + parts[1]));
        }

        public static class TransactionMapper extends Mapper<Object, Text, Text, Text> {
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String record = value.toString();
                String[] parts = record.split(",");
                context.write(new Text(parts[2]), new Text("txns\t" + parts[5]));
            }
        }

        public static class ReducerJoin extends Reducer<Text, Text, Text, Text> {
            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                    InterruptedException {
                String name = "";
                double sum = 0.0;
                int count = 0;
                for (Text t: values) {
                    String[] parts = t.toString().split("\t");
                    if (parts[0].equals("txns")) {
                        count++;
                        sum += Float.parseFloat(parts[1]);
                    } else if (parts[0].equals("custs")) {
                        name = parts[1];
                    }
                    String data = "No of Transactions: " + count + ". Total Transactions: "+sum;
                    context.write(new Text(name), new Text(data));
                }
            }
        }

        public static void main(String[] args) {
            try {
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf, "MapReduce Side-Join");

                job.setJarByClass(ReduceJoin.class);
                job.setReducerClass(ReducerJoin.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomerMapper.class);
                MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, TransactionMapper.class);

                Path outputPath = new Path(args[2]);

                FileOutputFormat.setOutputPath(job, outputPath);
                outputPath.getFileSystem(conf).delete(outputPath, true);
                System.exit(job.waitForCompletion(true) ? 0 : 1);

            } catch (IOException | InterruptedException | ClassNotFoundException e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
