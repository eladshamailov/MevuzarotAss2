import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * The class re represents a related pair with it's decade
 */
public class Job1KeyPair implements WritableComparable<Job1KeyPair>
{
    public static Text star = new Text("*");
    public static Text minus = new Text("-");

    private Text rightWord;
    private Text leftWord;

    /**
     * default constructor
     */
    public Job1KeyPair()
    {
        leftWord = new Text();
        rightWord = new Text();
    }

    /**
     * constructor
     * @param leftWord the left word in the pair
     * @param rightWord the right word in the pair
     */
    public Job1KeyPair(Text leftWord, Text rightWord)
    {
        this.leftWord = leftWord;
        this.rightWord = rightWord;
    }

    public Text getRightWord() {
        return rightWord;
    }

    public void setRightWord(Text rightWord) {
        this.rightWord = rightWord;
    }

    public Text getLeftWord() {
        return leftWord;
    }

    public void setLeftWord(Text leftWord) {
        this.leftWord = leftWord;
    }

    @Override
    public String toString()
    {
        return getLeftWord() + " " + getRightWord();
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
    public int compareTo(Job1KeyPair other)
    {
        int ans = leftWord.compareTo(other.leftWord);
        if (ans == 0)
        {
            if (rightWord.compareTo(Job1KeyPair.star) == 0 && other.rightWord.compareTo(Job1KeyPair.minus) == 0)
            {
                ans = -1;
            }
            else if (rightWord.compareTo(Job1KeyPair.minus) == 0 && other.rightWord.compareTo(Job1KeyPair.star) == 0)
            {
                ans = 1;
            }
            else
            {
                ans = rightWord.compareTo(other.rightWord);
            }
        }
        return ans;
    }
}