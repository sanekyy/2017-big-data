package sanekyy.tasks.nthWord;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, IntWritable, TextWithCountWritable>{

    private static int N;
    private static IntWritable ONE = new IntWritable(1);
    private int count = 0;

    static void setN(int N){
        Mapper.N = N;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        count = 0;
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        final String line = value.toString();

        int pos = line.indexOf(0x09);

        int inputCount = Integer.valueOf(line.substring(0, pos));
        String inputString = line.substring(pos+1);

        if (count <= N){
            context.write(ONE, new TextWithCountWritable(inputString, inputCount));
            count++;
        }
    }
}