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
public class Job3Value implements WritableComparable<Job3Value>
{
    private Job3KeyPair pair;
    private LongWritable leftOcc;
    private LongWritable rightOcc;
    private LongWritable totalOcc;
    private IntWritable year;

    /**
     * default constructor
     */
    public Job3Value()
    {
        pair = new Job3KeyPair();
        leftOcc = new LongWritable();
        rightOcc = new LongWritable();
        totalOcc = new LongWritable();
        year = new IntWritable();
    }

    public Job3Value(Job3KeyPair pair, LongWritable leftOcc, LongWritable rightOcc, LongWritable totalOcc, IntWritable year)
    {
        this.pair = pair;
        this.leftOcc = leftOcc;
        this.rightOcc = rightOcc;
        this.totalOcc = totalOcc;
        this.year = year;
    }

    public Job3KeyPair getPair() { return pair; }

    public LongWritable getLeftOcc() {
        return leftOcc;
    }

    public LongWritable getRightOcc() {
        return rightOcc;
    }

    public LongWritable getTotalOcc() {
        return totalOcc;
    }

    public IntWritable getYear() {
        return year;
    }

    @Override
    public String toString()
    {
        return pair.getLeftWord() + " " + pair.getRightWord() + " " + leftOcc + " " + rightOcc + " " + totalOcc + " " + year;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        pair.write(out);
        leftOcc.write(out);
        rightOcc.write(out);
        totalOcc.write(out);
        year.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        pair.readFields(in);
        leftOcc.readFields(in);
        rightOcc.readFields(in);
        totalOcc.readFields(in);
        year.readFields(in);
    }

    @Override
    public int compareTo(Job3Value other) {
        int ans = pair.getLeftWord().compareTo(other.pair.getLeftWord());
        if (ans == 0)
        {
            ans = pair.getRightWord().compareTo(other.pair.getRightWord());
        }
        return ans;
    }

}