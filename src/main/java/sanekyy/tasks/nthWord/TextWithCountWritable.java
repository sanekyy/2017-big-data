package sanekyy.tasks.nthWord;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextWithCountWritable implements WritableComparable<TextWithCountWritable>, Cloneable {
    private String text;
    private int count;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(count);
        dataOutput.writeUTF(text);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        count = dataInput.readInt();
        text = dataInput.readUTF();
    }

    public String getText() {
        return text;
    }

    public int getCount() {
        return count;
    }

    TextWithCountWritable(){
        // should be
    }

    public TextWithCountWritable(String text, int count) {
        this.text = text;
        this.count = count;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null){
            return false;
        }
        if (other == this){
            return true;
        }
        if (!(other instanceof TextWithCountWritable)){
            return false;
        }
        TextWithCountWritable otherMyClass = (TextWithCountWritable)other;
        if (otherMyClass.count != count){
            return false;
        }
        if (!otherMyClass.text.equals(text)){
            return false;
        }
        return true;
    }

    @Override
    protected TextWithCountWritable clone() {
        return new TextWithCountWritable(text, count);
    }

    @Override
    public int compareTo(TextWithCountWritable o) {
        if (equals(o)){
            return 0;
        }
        int intCompare = Integer.compare(count, o.count);
        return (intCompare == 0) ? Integer.compare(this.hashCode(), o.hashCode()) : -intCompare;
    }
}
