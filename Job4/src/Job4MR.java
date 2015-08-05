import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Job4MR
{
    public static class MapJob4 extends Mapper<LongWritable, Text, Job4KeyPair, Job4Value>
    {
        /**
         * @param key     the row number of the dataset stored as a LongWritable and the value is
         *                the raw data stored as TextWritable.
         * @param value   a tab separated string containing the following fields:
         *                pair - a space seperated text representing "leftWord rightWord" of the pair
         *
         *                year - the year that the word was written
         *                normalizedPmi - the pair's pmi
         *                sumOfPmis - the sum of all pmis for current decade
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
                String[] data = dataSetLine[1].split(" ");

                if (pair.length == 2 && data.length == 2)
                {
                    IntWritable year = new IntWritable(Integer.parseInt(data[0]));
                    DoubleWritable normalizedPmi = new DoubleWritable(Double.parseDouble(data[1]));

                    // same concept as in the first job. we send each pair twice. for a <*,*> pair,
                    // we will sum the number of occurrences that get to the reducer with the pair.
                    // for a <leftWord,rightWord> pair, we would save what we summed this far to the next mapper.
                    // *** we know that a star will get to the reducer before the pair because we used
                    // NaturalKeyGroupingComparator and CompositeKeyComparator
                    Job4KeyPair keyPairA = new Job4KeyPair(new Text(pair[0]),new Text(pair[1]));
                    Job4KeyPair keyPairB = new Job4KeyPair(new Text("*"), new Text("*"));

                    Job4Value val = new Job4Value(keyPairA, year, normalizedPmi);

                    context.write(keyPairA, val);
                    context.write(keyPairB, val);
                }
            }
        }

        public static class PartitionerJob4 extends Partitioner<Job4KeyPair, Job4Value>
        {

            /**
             * send the record to the reducer according to the year
             */
            @Override
            public int getPartition(Job4KeyPair key, Job4Value value, int numPartitions)
            {
                // set the reducer according to the decade
                return (value.getYear().get() - 1500) / 10 % numPartitions;
            }
        }

        public static class ReduceJob4 extends Reducer<Job4KeyPair, Job4Value, Job4KeyPair, Text>
        {
            private double minPmi;
            private double relMinPmi;

            @Override
            public void setup(Context context)
            {
                // read the K value from the configuration
                minPmi = Double.parseDouble(context.getConfiguration().get("MIN_PMI","0.95"));
                relMinPmi = Double.parseDouble(context.getConfiguration().get("REL_MIN_PMI", "0.3"));
            }
            protected void reduce(Job4KeyPair key, Iterable<Job4Value> values, Context context) throws IOException, InterruptedException
            {
                //accumulate all the values of this key
                double sumOfPmis = 0;

                for (Job4Value value : values)
                {
                    double npmi = value.getNormalizedPmi().get();
                    if (key.getLeftWord().compareTo(Job4KeyPair.star) == 0) //<*,*>
                    {
                        sumOfPmis += npmi;
                    }
                    else //<leftWord,rightWord>
                    {
                        boolean isCollocation = value.getNormalizedPmi().get() >= minPmi ||
                                ((npmi / sumOfPmis) >= relMinPmi);

                        if (isCollocation)
                        {
                            context.write(key, new Text(npmi + " " + value.getYear().get()));
                        }
                    }
                }
            }
        }
    }
}
