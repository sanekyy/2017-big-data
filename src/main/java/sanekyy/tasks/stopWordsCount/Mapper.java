package sanekyy.tasks.stopWordsCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable>{

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        final String line = value.toString();

        int pos = line.indexOf(0x09);

        String inputString = line.substring(0, pos);
        int inputCount = Integer.valueOf(line.substring(pos+1));

        context.write(new Text(inputString.toLowerCase()), new IntWritable(inputCount));
    }
}