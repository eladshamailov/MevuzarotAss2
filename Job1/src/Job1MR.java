import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1MR
{
    public static class MapJob1 extends Mapper<LongWritable, Text, Job1KeyPair, Job1ValuePair>
    {
        private List<String> stopWords;
        private boolean removeStopWords;


        /**
         * This function is to prevent a false collocation
         * @param left left word of pair
         * @param right right word of pair
         * @return true if both words in pair are numbers
         */
        private boolean isPairOfNumbers(String left, String right)
        {
            boolean parsable = true;
            try
            {
                Integer.parseInt(left);
                Integer.parseInt(right);
            }
            catch (NumberFormatException e)
            {
                parsable = false;
            }
            return parsable;
        }

        /**
         * Called once at the beginning of the task.
         */
        protected void setup(Context context) throws IOException, InterruptedException
        {
            removeStopWords = context.getConfiguration().getBoolean("removeStopWords", true);
            stopWords = Arrays.asList(new StopWords().getStopWords(removeStopWords));
        }

        /**
         * @param key     the row number of the dataset stored as a LongWritable and the value is
         *                the raw data stored as TextWritable.
         * @param value   a tab separated string containing the following fields:
         *                n-gram - The actual n-gram
         *                year - The year for this aggregation
         *                occurrences - The number of times this n-gram appeared in this year
         *                pages - The number of pages this n-gram appeared on in this year
         *                books - The number of books this n-gram appeared in during this year
         *                The n-gram field is a space separated representation of the tuple.
         * @param context the context to write the answer to
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();

            // get the data set line values that are separated by tab
            String[] dataSetLine = line.split("\t");

            // Verify that the data set line contains at list values
            // n-gram, year, occurrences .
            if (dataSetLine.length > 2)
            {
                //get the rest of the data from the data set line
                String[] twoGram = dataSetLine[0].split(" ");
                IntWritable year = new IntWritable(Integer.parseInt(dataSetLine[1]));

                if (twoGram.length == 2) {
                    if (!stopWords.contains(twoGram[0]) && !stopWords.contains(twoGram[1])
                            && !isPairOfNumbers(twoGram[0],twoGram[1]))
                    {
                        long occurrences = Long.parseLong(dataSetLine[2]);

                        Text leftWord, rightWord;

                        leftWord = new Text(twoGram[0]);
                        rightWord = new Text(twoGram[1]);

                        Job1ValuePair valuePairLeft = new Job1ValuePair(new Job1KeyPair(leftWord, rightWord), year, new LongWritable(occurrences), new BooleanWritable(true));
                        Job1ValuePair valuePairRight = new Job1ValuePair(new Job1KeyPair(leftWord, rightWord), year, new LongWritable(occurrences), new BooleanWritable(false));

                        Job1KeyPair keyPairA = new Job1KeyPair(leftWord, Job1KeyPair.star);
                        Job1KeyPair keyPairB = new Job1KeyPair(leftWord, Job1KeyPair.minus);
                        Job1KeyPair keyPairC = new Job1KeyPair(rightWord, Job1KeyPair.star);
                        Job1KeyPair keyPairD = new Job1KeyPair(rightWord, Job1KeyPair.minus);

                        context.write(keyPairA, valuePairLeft);
                        context.write(keyPairB, valuePairLeft);
                        context.write(keyPairC, valuePairRight);
                        context.write(keyPairD, valuePairRight);
                    }
                }
            }
        }

        public static class PartitionerJob1 extends Partitioner<Job1KeyPair, Job1ValuePair>
        {

            /**
             * send the recored to the reducer according to the year
             */
            @Override
            public int getPartition(Job1KeyPair key, Job1ValuePair value, int numPartitions)
            {
                // set the reducer according to the decade
                return (value.getYear() - 1500) / 10 % numPartitions;
            }
        }


        public static class ReduceJob1 extends Reducer<Job1KeyPair, Job1ValuePair, Job1KeyPair, Text>
        {

            protected void reduce(Job1KeyPair key, Iterable<Job1ValuePair> values, Context context) throws IOException, InterruptedException
            {
                //accumulate all the values of this key
                long sumOfLeftOcc = 0;
                long sumOfRightOcc = 0;

                for (Job1ValuePair value : values)
                {
                    if (key.getRightWord().compareTo(Job1KeyPair.star) == 0) //<x,*>
                    {
                        if (value.isLeft().get() == true)
                        {
                            sumOfLeftOcc += value.getOccurrences().get();
                        }
                        else
                        {
                            sumOfRightOcc += value.getOccurrences().get();
                        }
                    }
                    else //<x,->
                    {
                        long left = value.isLeft().get() ? sumOfLeftOcc : 0;
                        long right = !value.isLeft().get() ? sumOfRightOcc : 0;

                        context.write(value.getPair(), new Text(left + " " + right + " " + value.getYear() + " " + value.getOccurrences()));
                    }
                }
            }
        }
    }
}
