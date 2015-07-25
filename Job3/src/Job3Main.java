import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job3Main {

    private static final int NUM_OF_DECADES = 12;

    public static void main(String[] args) throws Exception
    {

        Configuration conf = new Configuration();
        conf.set("mapred.max.split.size", "5242880");//set split size to 5mb

        Job job = new Job(conf, "Job3MapReduce");
        job.setInputFormatClass(TextInputFormat.class);
        job.setJarByClass(Job3MR.class);
        job.setMapperClass(Job3MR.MapJob3.class);
        job.setMapOutputKeyClass(Job3KeyPair.class);
        job.setMapOutputValueClass(Job3Value.class);
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);
        job.setPartitionerClass(Job3MR.MapJob3.PartitionerJob3.class);
        job.setReducerClass(Job3MR.MapJob3.ReduceJob3.class);
        job.setNumReduceTasks(NUM_OF_DECADES);
        job.setOutputKeyClass(Job3KeyPair.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean flag = job.waitForCompletion(true);
        if (!flag)
        {
            System.err.println("Problem With Map-reduce 1");
            return;
        }
        else
         {
            System.out.println("job 1 finished successfully");
        }
    }

    public static class NaturalKeyGroupingComparator extends WritableComparator
    {

        protected NaturalKeyGroupingComparator() {
            super(Job3KeyPair.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2)
        {
            return 0;
        }
    }

    public static class CompositeKeyComparator extends WritableComparator
    {

        protected CompositeKeyComparator() {
            super(Job3KeyPair.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            Job3KeyPair k1 = (Job3KeyPair)w1;
            Job3KeyPair k2 = (Job3KeyPair)w2;

            int result = k1.getLeftWord().toString().compareTo(k2.getLeftWord().toString());
            if (result == 0)
            {
                result = k1.getRightWord().toString().compareTo(k2.getRightWord().toString());
            }
            return result;
        }
    }
}

