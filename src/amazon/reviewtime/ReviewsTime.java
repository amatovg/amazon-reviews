package amazon.reviewtime;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReviewsTime extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ReviewsTime(), args);
        System.exit(res);
    }
    public Job getJob(String input, String output, String precision) throws IOException {
        // Create a new job.
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(ReviewsTime.class);
        job.setJobName("Reviews Time");

        // set mapper / reducer classes.
        job.setMapperClass(ReviewsTimeMapper.class);
        job.setReducerClass(ReviewsTimeReducer.class);
        job.setCombinerClass(ReviewsTimeReducer.class);

        // Define output types.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Specify various job-specific parameters.
        job.getConfiguration().set("unit", precision);

        // Define input and output folders.
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(output), true);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job;
    }

    @Override
    public int run(String[] args) throws Exception{

        if (args.length != 3) {
            System.err.println("Usage: CMD <input path> <output path> <precision>");
            return 0;
        }
        Job job = this.getJob(args[0], args[1],args[2]);
        job.waitForCompletion(true);
        return 0;


    }
}
