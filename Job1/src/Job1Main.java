import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job1Main {

    private static final int NUM_OF_DECADES = 52;

    public static void main(String[] args) throws Exception
    {

        Configuration conf = new Configuration();
        conf.set("mapred.max.split.size", "10485760");//set split size to 10mb
        conf.set("includeStopWords",args[3]);

        Job job = new Job(conf, "Job1MapReduce");
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setJarByClass(Job1MR.class);
        job.setMapperClass(Job1MR.MapJob1.class);
        job.setMapOutputKeyClass(Job1KeyPair.class);
        job.setMapOutputValueClass(Job1ValuePair.class);
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);
        job.setPartitionerClass(Job1MR.MapJob1.PartitionerJob1.class);
        job.setReducerClass(Job1MR.MapJob1.ReduceJob1.class);
        job.setNumReduceTasks(NUM_OF_DECADES);
        job.setOutputKeyClass(Job1KeyPair.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean flag = job.waitForCompletion(true);
        if(!flag)
        {
            System.err.println("Problem With Map-reduce 1");
            return;
        }
        else{
            System.out.println("job 1 finished successfully");
        }
    }

    public static class NaturalKeyGroupingComparator extends WritableComparator {

        protected NaturalKeyGroupingComparator() {
            super(Job1KeyPair.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            Job1KeyPair k1 = (Job1KeyPair)w1;
            Job1KeyPair k2 = (Job1KeyPair)w2;

            return k1.getLeftWord().toString().compareTo(k2.getLeftWord().toString());
        }
    }

    public static class CompositeKeyComparator extends WritableComparator
    {

        protected CompositeKeyComparator() {
            super(Job1KeyPair.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2)
        {
            Job1KeyPair k1 = (Job1KeyPair)w1;
            Job1KeyPair k2 = (Job1KeyPair)w2;

            int result = k1.getLeftWord().toString().compareTo(k2.getLeftWord().toString());
            if (result == 0)
            {
                result = k1.getRightWord().toString().compareTo(k2.getRightWord().toString());
            }
            return result;
        }
    }
}

