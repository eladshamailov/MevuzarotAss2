import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Job2MR
{
    public static class MapJob2 extends Mapper<LongWritable, Text, Job2KeyPair, LongWritable>
    {

        /**
         * @param key     the row number of the dataset stored as a LongWritable and the value is
         *                the raw data stored as TextWritable.
         * @param value   a tab separated string containing the following fields:
         *                pair - the pair of words
         *                leftOcc - The number of occurences of the left word in the corpus
         *                rightOcc - The number of occurences of the right word in the corpus
         *                year - the year that the word was written
         *                occurrences - The number of times this 2-gram appeared in this year
         *                The n-gram field is a space separated representation of the tuple.
         * @param context the context to write the answer to
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();

            // get the data set line values that are separated by tab
            String[] dataSetLine = line.split("\t");

            // Verify that the data set line contains at least values
            // n-gram, year, occurrences
            if (dataSetLine.length == 2)
            {
                String[] pair = dataSetLine[0].split(" ");
                String[] occurrences = dataSetLine[1].split(" ");

                if (pair.length == 2 && occurrences.length == 4)
                {
                    Text leftWord = new Text(pair[0]);
                    Text rightWord = new Text(pair[1]);
                    LongWritable leftOcc = new LongWritable(Long.parseLong(occurrences[0]));
                    LongWritable rightOcc = new LongWritable(Long.parseLong(occurrences[1]));
                    IntWritable year = new IntWritable(Integer.parseInt(occurrences[2]));
                    Job2KeyPair keyPair = new Job2KeyPair(leftWord, rightWord, leftOcc, rightOcc, year);

                    context.write(keyPair, new LongWritable(Long.parseLong(occurrences[3])));
                }
            }
        }

        public static class PartitionerJob2 extends Partitioner<Job2KeyPair, LongWritable>
        {

            /**
             * send the recored to the reducer according to the year
             */
            @Override
            public int getPartition(Job2KeyPair key, LongWritable value, int numPartitions)
            {
                // set the reducer according to the decade
                return (key.getYear().get() - 1900) / 10 % numPartitions;
            }
        }


        public static class ReduceJob2 extends Reducer<Job2KeyPair, LongWritable, Text, Text>
        {
            protected void reduce(Job2KeyPair key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
            {
                //accumulate all the values of this key
                long totalOcc = 0;
                long leftOcc = 0;
                long rightOcc = 0;

                for (LongWritable value : values)
                {
                    long right = key.getRightOcc().get();
                    long left = key.getLeftOcc().get();

                    if (left == -1)
                    {
                        rightOcc += right;
                    }
                    else
                    {
                        leftOcc += left;
                    }

                    totalOcc += value.get();
                }
                Text pairKey = new Text(key.getLeftWord() + " " + key.getRightWord());
                totalOcc /= 2; //we summed occurrences of left & right word so we need to divide by 2.
                Text pairValue = new Text(leftOcc + " " + rightOcc + " " + totalOcc + " " + key.getYear());

                context.write(pairKey, pairValue);
            }
        }
    }
}
