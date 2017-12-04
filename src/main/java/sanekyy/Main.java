package sanekyy;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import sanekyy.tasks.namesPercent.NamesPercent;
import sanekyy.tasks.stopWordsCount.StopWordsCount;
import sanekyy.tasks.wordCount.WordCount;
import sanekyy.tasks.nthWord.NthWord;
import sanekyy.tasks.wordsOrder.WordsOrder;

import java.io.File;

public class Main {
    private static final String BASE_INPUT_PATH = "src/main/resources/input/";
    private static final String BASE_OUTPUT_PATH = "src/main/resources/output/";

    private static final String INPUT_WORDS_DIR = BASE_INPUT_PATH + "words";
    private static final String INPUT_TEST_DATA_DIR = BASE_INPUT_PATH + "testData";
    private static final String STOP_WORDS = BASE_INPUT_PATH + "stopWords/stop_words_en.txt";
    private static final String WORD_COUNT_DIR = BASE_OUTPUT_PATH + "wordCount";
    private static final String ORDERED_WORD_COUNT_DIR = BASE_OUTPUT_PATH + "orderedWordCount";
    private static final String WORD_COUNT_7TH_DIR = BASE_OUTPUT_PATH + "wordCount7th";
    private static final String STOP_WORDS_COUNT_DIR = BASE_OUTPUT_PATH + "stopWordsCount";
    private static final String NAMES_DIR = BASE_OUTPUT_PATH + "names";
    private static final String ORDERED_NAMES_DIR = BASE_OUTPUT_PATH + "orderedNames";
    private static final String NAMES_5TH_DIR = BASE_OUTPUT_PATH + "names5th";

    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File(BASE_OUTPUT_PATH));

        ToolRunner.run(
                new Configuration(),
                new WordCount(),
                new String[]{INPUT_WORDS_DIR, WORD_COUNT_DIR}
        );

        ToolRunner.run(
                new Configuration(),
                new WordsOrder(),
                new String[]{WORD_COUNT_DIR, ORDERED_WORD_COUNT_DIR}
        );

        ToolRunner.run(
                new Configuration(),
                new NthWord(),
                new String[]{ORDERED_WORD_COUNT_DIR, WORD_COUNT_7TH_DIR, "7"}
        );

        ToolRunner.run(
                new Configuration(),
                new StopWordsCount(),
                new String[]{WORD_COUNT_DIR, STOP_WORDS_COUNT_DIR, STOP_WORDS}
        );

        ToolRunner.run(
                new Configuration(),
                new NamesPercent(),
                new String[]{WORD_COUNT_DIR, NAMES_DIR}
        );

        ToolRunner.run(
                new Configuration(),
                new WordsOrder(),
                new String[]{NAMES_DIR, ORDERED_NAMES_DIR}
        );

        ToolRunner.run(
                new Configuration(),
                new NthWord(),
                new String[]{ORDERED_NAMES_DIR, NAMES_5TH_DIR, "5"}
        );
    }
}
