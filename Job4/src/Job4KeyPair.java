import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * The class re represents a related pair with it's decade
 */
public class Job4KeyPair implements WritableComparable<Job4KeyPair>
{
    public static Text star = new Text("*");

    private Text rightWord;
    private Text leftWord;

    /**
     * default constructor
     */
    public Job4KeyPair()
    {
        this.leftWord = new Text();
        this.rightWord = new Text();
    }

    /**
     * constructor
     * @param leftWord the left word in the pair
     * @param rightWord the right word in the pair
     */
    public Job4KeyPair(Text leftWord, Text rightWord)
    {
        this.leftWord = leftWord;
        this.rightWord = rightWord;
    }

    public Text getRightWord() {
        return rightWord;
    }

    public Text getLeftWord() {
        return leftWord;
    }

    @Override
    public String toString()
    {
        return leftWord + " " + rightWord;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        leftWord.write(out);
        rightWord.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        leftWord.readFields(in);
        rightWord.readFields(in);
    }

    @Override
    public int compareTo(Job4KeyPair other)
    {
        if (leftWord.compareTo(star) == 0 && rightWord.compareTo(star) != 0)
        {
            return -1;
        }
        else if (leftWord.compareTo(star) != 0 && rightWord.compareTo(star) == 0)
        {
            return 1;
        }
        else
        {
            int ans = leftWord.compareTo(other.getLeftWord());
            if (ans == 0)
            {
                ans = rightWord.compareTo(other.getRightWord());
            }
            return ans;
        }
    }
}