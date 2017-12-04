package sanekyy.tasks.nthWord;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, TextWithCountWritable, Text, IntWritable>{
    private SortedSet<TextWithCountWritable> setOfTextWithCount = new TreeSet<>();
    private static int N;

    static void setN(int N){
        Reducer.N = N;
    }

    @Override
    protected void reduce(IntWritable key, Iterable<TextWithCountWritable> values, Context context) throws IOException, InterruptedException {
        values.forEach((textWithCountWritable -> setOfTextWithCount.add(textWithCountWritable.clone())));
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        int i = 0;
        for (TextWithCountWritable textWithCountWritable : setOfTextWithCount){
            if (i == N - 1){
                context.write(new Text(textWithCountWritable.getText()), new IntWritable(textWithCountWritable.getCount()));
                break;
            }
            i++;
        }
    }
}
