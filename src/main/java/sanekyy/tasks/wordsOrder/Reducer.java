package sanekyy.tasks.wordsOrder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (final Text value : values) {
            context.write(key, value);
        }
    }
}
