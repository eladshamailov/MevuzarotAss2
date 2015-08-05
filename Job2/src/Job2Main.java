import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Job2Main {

    private static final int NUM_OF_DECADES = 52;

    public static void main(String[] args) throws Exception
    {

        Configuration conf = new Configuration();
        conf.set("mapred.max.split.size", "5242880");//set split size to 5mb

        Job job = new Job(conf, "Job2MapReduce");
        job.setInputFormatClass(TextInputFormat.class);
        job.setJarByClass(Job2MR.class);
        job.setMapperClass(Job2MR.MapJob2.class);
        job.setMapOutputKeyClass(Job2KeyPair.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setPartitionerClass(Job2MR.MapJob2.PartitionerJob2.class);
        job.setReducerClass(Job2MR.MapJob2.ReduceJob2.class);
        job.setNumReduceTasks(NUM_OF_DECADES);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean flag = job.waitForCompletion(true);
        if (!flag)
        {
            System.err.println("Problem With Map-reduce 2");
            return;
        }
        else{
            System.out.println("job 2 finished successfully");
        }
    }
}

