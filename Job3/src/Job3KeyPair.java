import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * The class re represents a related pair with it's decade
 */
public class Job3KeyPair implements WritableComparable<Job3KeyPair>
{
    public static Text star = new Text("*");

    private Text rightWord;
    private Text leftWord;

    /**
     * default constructor
     */
    public Job3KeyPair()
    {
        this.leftWord = new Text();
        this.rightWord = new Text();
    }

    /**
     * constructor
     * @param leftWord the left word in the pair
     * @param rightWord the right word in the pair
     */
    public Job3KeyPair(Text leftWord, Text rightWord)
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
        return getLeftWord() + " " + getRightWord();
    }

    @Override
    public void write(DataOutput out) throws IOException {

        this.leftWord.write(out);
        this.rightWord.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.leftWord.readFields(in);
        this.rightWord.readFields(in);
    }

    @Override
    public int compareTo(Job3KeyPair other)
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