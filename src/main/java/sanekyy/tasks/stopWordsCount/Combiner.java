package sanekyy.tasks.stopWordsCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Combiner extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int valuesCount = 0;

        for (final IntWritable value : values) {
            valuesCount += value.get();
        }

        context.write(key, new IntWritable(valuesCount));
    }
}
