package sanekyy.tasks.stopWordsCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.File;
import java.nio.file.Files;

import static sanekyy.tasks.stopWordsCount.Reducer.setStopWords;

public class StopWordsCount extends Configured implements Tool {

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = this.getConf();
        final Job job = Job.getInstance(conf, "StopWordsCount");

        // третьим аргументом приходит файл стоп-слов
        setStopWords(args[2]);

        job.setJarByClass(StopWordsCount.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(Mapper.class);
        job.setCombinerClass(Combiner.class);
        job.setReducerClass(Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // читаем из выходной папки wordcount
        FileInputFormat.addInputPath(job, new Path(args[0]));

        // в выходную папку положится пустой файл (в context ничего не пишем)
        // и в этой же папке создастся файл "output", в котором будет результат работы программы
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean success = job.waitForCompletion(true);

        long stopWordsCount = job.getCounters().findCounter(Reducer.MY_COUNTERS.STOP_WORDS_COUNT).getValue();
        long wordsCount = job.getCounters().findCounter(Reducer.MY_COUNTERS.WORDS_COUNT).getValue();
        double percent = stopWordsCount / (double)wordsCount * 100;

        File file = new File(args[1] + "/output");
        Files.write(file.toPath(), String.valueOf(percent).getBytes());

        return success ? 0 : 1;
    }
}
