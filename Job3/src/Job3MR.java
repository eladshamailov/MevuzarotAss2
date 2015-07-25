import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Job3MR
{
    public static class MapJob3 extends Mapper<LongWritable, Text, Job3KeyPair, Job3Value>
    {
        /**
         * @param key     the row number of the dataset stored as a LongWritable and the value is
         *                the raw data stored as TextWritable.
         * @param value   a tab separated string containing the following fields:
         *                pair - a space seperated text representing "leftWord rightWord" of the pair
         *                leftOcc - The number of occurences of the left word in the corpus
         *                rightOcc - The number of occurences of the right word in the corpus
         *                occurrences - The number of times this 2-gram appeared in this year
         *                year - the year that the word was written
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
                    LongWritable leftOcc = new LongWritable(Long.parseLong(occurrences[0]));
                    LongWritable rightOcc = new LongWritable(Long.parseLong(occurrences[1]));
                    LongWritable totalOcc = new LongWritable(Long.parseLong(occurrences[2]));
                    IntWritable year = new IntWritable(Integer.parseInt(occurrences[3]));

                    // same concept as in the first job. we send each pair twice. for a <*,*> pair,
                    // we will sum the number of occurrences that get to the reducer with the pair.
                    // for a <-,-> pair, we would save what we summed this far to the next mapper.
                    // *** we know that a star will get to the reducer before the minus because we used
                    // NaturalKeyGroupingComparator and CompositeKeyComparator
                    Job3KeyPair keyPairA = new Job3KeyPair(new Text(pair[0]),new Text(pair[1]));
                    Job3KeyPair keyPairB = new Job3KeyPair(new Text("*"), new Text("*"));

                    Job3Value val = new Job3Value(keyPairA, leftOcc, rightOcc, totalOcc, year);

                    context.write(keyPairA, val);
                    context.write(keyPairB, val);
                }
            }
        }

        public static class PartitionerJob3 extends Partitioner<Job3KeyPair, Job3Value>
        {

            /**
             * send the record to the reducer according to the year
             */
            @Override
            public int getPartition(Job3KeyPair key, Job3Value value, int numPartitions)
            {
                // set the reducer according to the decade
                return (value.getYear().get() - 1900) / 10 % numPartitions;
            }
        }

        public static class ReduceJob3 extends Reducer<Job3KeyPair, Job3Value, Job3KeyPair, Text>
        {

            protected void reduce(Job3KeyPair key, Iterable<Job3Value> values, Context context) throws IOException, InterruptedException
            {
                //accumulate all the values of this key
                long sumOfOccurrences = 0;
                System.out.println("Printing reducer:");
                for (Job3Value value : values)
                {
                    if (key.getLeftWord().compareTo(Job3KeyPair.star) == 0) //< , >
                    {
                        sumOfOccurrences += value.getTotalOcc().get();
                    }
                    else //<leftWord,rightWord>
                    {
                        double pmi = Math.log(value.getTotalOcc().get()) + Math.log(sumOfOccurrences) -
                                Math.log(value.getLeftOcc().get()) - Math.log(value.getRightOcc().get());
                        Text val = new Text(value.getLeftOcc().get() + " " + value.getRightOcc() + " " + value.getTotalOcc() + " " + sumOfOccurrences + " " + value.getYear());

                        context.write(key, val);
                    }
                }
            }
        }
    }
}
