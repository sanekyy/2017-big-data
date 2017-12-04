package sanekyy.tasks.wordsOrder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, IntWritable, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        final String line = value.toString();

        String[] data = line.split("\t");

        context.write(new IntWritable(Integer.valueOf(data[1])), new Text(data[0]));
    }
}