package sanekyy.tasks.stopWordsCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, IntWritable, Text, IntWritable>{

    private static Set<String> stopWords;

    static void setStopWords(String stopWordsFile){
        stopWords = new HashSet<>();

        try {
            Files
                    .lines(Paths.get(stopWordsFile), StandardCharsets.UTF_8)
                    .forEach(stopWords::add);
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        boolean isStopWord = stopWords.contains(key.toString());

        for (final IntWritable value : values) {
            context.getCounter(MY_COUNTERS.WORDS_COUNT).increment(value.get());

            if (isStopWord){
                context.getCounter(MY_COUNTERS.STOP_WORDS_COUNT).increment(value.get());
            }
        }
    }

    enum MY_COUNTERS {
        WORDS_COUNT,
        STOP_WORDS_COUNT
    }
}
