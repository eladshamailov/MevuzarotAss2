import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1MR {
    public static class MapJob1 extends Mapper<LongWritable, Text, Job1KeyPair, Job1ValuePair> {
        private List<String> stopWords;
        private boolean includeStopWords;

        /**
         * Called once at the beginning of the task.
         */
        protected void setup(Context context) throws IOException, InterruptedException {
            includeStopWords = context.getConfiguration().getBoolean("includeStopWords", true);
            stopWords = Arrays.asList(new StopWords().getStopWords(includeStopWords));
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
                //get the year and validate it's pass 1900
                IntWritable year = new IntWritable(Integer.parseInt(dataSetLine[1]));
                if (year.get() > 1900 )
                {
                    //get the rest of the data from the data set line
                    String[] twoGram = dataSetLine[0].split(" ");

                    if (twoGram.length == 2) {
                        if (!stopWords.contains(twoGram[0]) && !stopWords.contains(twoGram[1]))
                        {
                            long occurrences = Long.parseLong(dataSetLine[2]);

                            Text leftWord, rightWord;

                            leftWord = new Text(twoGram[0]);
                            rightWord = new Text(twoGram[1]);


                            Job1ValuePair valuePair = new Job1ValuePair(new Job1KeyPair(leftWord, rightWord), year, new LongWritable(occurrences));
                            Job1KeyPair keyPairA = new Job1KeyPair(leftWord, Job1KeyPair.star);
                            Job1KeyPair keyPairB = new Job1KeyPair(leftWord, Job1KeyPair.minus);
                            Job1KeyPair keyPairC = new Job1KeyPair(rightWord, Job1KeyPair.star);
                            Job1KeyPair keyPairD = new Job1KeyPair(rightWord, Job1KeyPair.minus);


                            context.write(keyPairA, valuePair);
                            context.write(keyPairB, valuePair);
                            context.write(keyPairC, valuePair);
                            context.write(keyPairD, valuePair);
                        }
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
                return (value.getYear() - 1900) / 10 % numPartitions;
            }
        }


        public static class ReduceJob1 extends Reducer<Job1KeyPair, Job1ValuePair, Job1KeyPair, Text>
        {

            protected void reduce(Job1KeyPair key, Iterable<Job1ValuePair> values, Context context) throws IOException, InterruptedException
            {
                //accumulate all the values of this key
                long sumOfValues = 0;

                for (Job1ValuePair value : values)
                {
                    if (key.getRightWord().compareTo(Job1KeyPair.star) == 0) //<x,*>
                    {
                        sumOfValues += value.getOccurrences().get();
                    } else //<x,->
                    {
                        long left = isLeftWordInPair(value.getPair(), key.getLeftWord()) ? sumOfValues : -1;
                        long right = left == -1 ? sumOfValues : -1;
                        context.write(value.getPair(), new Text(left + " " + right + " " + value.getYear() + " " + value.getOccurrences()));
                    }
                }
            }

            private boolean isLeftWordInPair(Job1KeyPair pair, Text leftWord)
            {
                return leftWord.compareTo(pair.getLeftWord()) == 0;
            }

        }
    }
}
