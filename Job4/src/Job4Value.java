import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * The class re represents a related pair with it's decade
 */
public class Job4Value implements WritableComparable<Job4Value>
{
    private Job4KeyPair pair;
    private IntWritable year;
    private DoubleWritable normalizedPmi;

    /**
     * default constructor
     */
    public Job4Value()
    {
        pair = new Job4KeyPair();
        year = new IntWritable();
        normalizedPmi = new DoubleWritable();
    }

    public Job4Value(Job4KeyPair pair, IntWritable year, DoubleWritable normalizedPmi)
    {
        this.pair = pair;
        this.year = year;
        this.normalizedPmi = normalizedPmi;
    }

    public Job4KeyPair getPair() { return pair; }

    public IntWritable getYear() {
        return year;
    }

    public DoubleWritable getNormalizedPmi() { return normalizedPmi; }

    @Override
    public String toString()
    {
        return pair.getLeftWord() + " " + pair.getRightWord() + " " + year + " " + normalizedPmi;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        pair.write(out);
        year.write(out);
        normalizedPmi.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pair.readFields(in);
        year.readFields(in);
        normalizedPmi.readFields(in);
    }

    @Override
    public int compareTo(Job4Value other) {
        int ans = pair.getLeftWord().compareTo(other.pair.getLeftWord());
        if (ans == 0)
        {
            ans = pair.getRightWord().compareTo(other.pair.getRightWord());
        }
        return ans;
    }

}