package sanekyy.tasks.namesPercent;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, TextWithCountWritable, Text, IntWritable>{

    private static final Pattern namePattern = Pattern.compile("^[A-Z][a-z0-9]*$");

    @Override
    protected void reduce(Text key, Iterable<TextWithCountWritable> values, Context context) throws IOException, InterruptedException {
        int sumAllForms = 0;
        int rightFormCount = 0;
        String rightFormText = null;

        for (final TextWithCountWritable value : values){
            sumAllForms += value.getCount();

            if (rightFormText == null){
                Matcher matcher = namePattern.matcher(value.getText());
                if (matcher.matches()){
                    rightFormText = value.getText();
                    rightFormCount = value.getCount();
                }
            }
        }

        if (rightFormText == null){
            return;
        }

        if (rightFormCount / (double)sumAllForms >= 0.995){
            context.write(new Text(rightFormText), new IntWritable(rightFormCount));
        }
    }
}
