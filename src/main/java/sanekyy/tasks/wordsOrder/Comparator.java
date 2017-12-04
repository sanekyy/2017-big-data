package sanekyy.tasks.wordsOrder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparator;

public class Comparator extends WritableComparator {
    protected Comparator() {
        super(IntWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return -Integer.compare(readInt(b1, s1), readInt(b2, s2));
    }
}
