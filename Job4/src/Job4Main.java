import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;

public class Job4Main
{
    private static final int NUM_OF_DECADES = 52;

    public static void main(String[] args) throws Exception
    {
        AWSCredentials credentials = new PropertiesCredentials(Job4Main.class.getResourceAsStream("AwsCredentials.properties"));
        AmazonS3 s3 = new AmazonS3Client(credentials);
        String bucketName = "ass2-yoav-eliran";

        Configuration conf = new Configuration();
        conf.set("mapred.max.split.size", "5242880");//set split size to 5mb
        conf.set("MIN_PMI", args[3]);
        conf.set("REL_MIN_PMI", args[4]);

        Job job = new Job(conf, "Job4MapReduce");
        job.setInputFormatClass(TextInputFormat.class);
        job.setJarByClass(Job4MR.class);
        job.setMapperClass(Job4MR.MapJob4.class);
        job.setMapOutputKeyClass(Job4KeyPair.class);
        job.setMapOutputValueClass(Job4Value.class);
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);
        job.setSortComparatorClass(CompositeKeyComparator.class);
        job.setPartitionerClass(Job4MR.MapJob4.PartitionerJob4.class);
        job.setReducerClass(Job4MR.MapJob4.ReduceJob4.class);
        job.setNumReduceTasks(NUM_OF_DECADES);
        job.setOutputKeyClass(Job4KeyPair.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean flag = job.waitForCompletion(true);
        if (!flag)
        {
            System.err.println("Error: Map-reduce 4");
            return;
        }
        else
        {
            ListObjectsRequest lor = new ListObjectsRequest()
                    .withBucketName(bucketName)
                    .withPrefix("outputJob4/");
            ObjectListing objects = s3.listObjects(lor);

            for (S3ObjectSummary objectSummary : objects.getObjectSummaries())
            {
                if (objectSummary.getSize() == 0)
                {
                    s3.deleteObject(bucketName, objectSummary.getKey());
                    continue;
                }
                else
                {
                    //18 is because of outputJob4/part-r-[#....]
                    String sequenceNumOfDecade = objectSummary.getKey().substring(18);
                    //number of decade from 1500
                    int decadeOffset = Integer.parseInt(sequenceNumOfDecade);
                    int decade = 150 + decadeOffset;
                    s3.copyObject(new CopyObjectRequest(bucketName, objectSummary.getKey(), bucketName, objectSummary.getKey().substring(0, 11) + "Decade_" + decade * 10 + "-" + ((decade * 10) + 9)));
                    s3.deleteObject(new DeleteObjectRequest(bucketName,objectSummary.getKey()));
                }

                System.out.println("job 4 finished successfully");
            }
        }
    }

    public static class NaturalKeyGroupingComparator extends WritableComparator
    {

        protected NaturalKeyGroupingComparator() {
            super(Job4KeyPair.class, true);
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
            super(Job4KeyPair.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            Job4KeyPair k1 = (Job4KeyPair)w1;
            Job4KeyPair k2 = (Job4KeyPair)w2;

            return k1.compareTo(k2);
        }
    }
}

