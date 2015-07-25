import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The class re represents a related pair with it's decade
 */
public class Job2KeyPair implements WritableComparable<Job2KeyPair>
{
    private Text leftWord;
    private Text rightWord;
    private LongWritable leftOcc;
    private LongWritable rightOcc;
    private IntWritable year;

    /**
     * default constructor
     */
    public Job2KeyPair()
    {
        leftWord = new Text();
        rightWord = new Text();
        leftOcc = new LongWritable();
        rightOcc = new LongWritable();
        year = new IntWritable();
    }

    public Job2KeyPair(Text leftWord, Text rightWord, LongWritable leftOcc, LongWritable rightOcc, IntWritable year)
    {
        this.leftWord = leftWord;
        this.rightWord = rightWord;
        this.rightOcc = rightOcc;
        this.leftOcc = leftOcc;
        this.year = year;
    }

    public Text getRightWord() {
        return rightWord;
    }

    public Text getLeftWord() {
        return leftWord;
    }

    public LongWritable getLeftOcc() {
        return leftOcc;
    }

    public LongWritable getRightOcc() {
        return rightOcc;
    }

    public IntWritable getYear() {
        return year;
    }
    @Override
    public String toString()
    {
        return leftWord + " " + rightWord + " " + leftOcc + " " + rightOcc + " " + year;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        leftWord.write(out);
        rightWord.write(out);
        leftOcc.write(out);
        rightOcc.write(out);
        year.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        leftWord.readFields(in);
        rightWord.readFields(in);
        leftOcc.readFields(in);
        rightOcc.readFields(in);
        year.readFields(in);
    }

    @Override
    public int compareTo(Job2KeyPair other) {
        int ans = leftWord.compareTo(other.leftWord);
        if (ans == 0)
        {
            ans = rightWord.compareTo(other.rightWord);
        }
        return ans;
    }
}