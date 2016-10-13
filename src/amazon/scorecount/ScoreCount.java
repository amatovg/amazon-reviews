package amazon.scorecount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ScoreCount extends Configured implements Tool{

    public Job getJob(String input, String output) throws IOException {
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(ScoreCount.class);
        job.setJobName("Score Count");

        // set mapper / reducer classes.
        job.setMapperClass(ScoreCountMapper.class);
        job.setReducerClass(ScoreCountReducer.class);
        job.setCombinerClass(ScoreCountReducer.class);

        // Define output types.
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);


        // Define input and output folders.
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(output), true);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job;
    }
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ScoreCount(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception{

        if (args.length != 2) {
            System.err.println("Usage: CMD <input path> <output path>");
            return 0;
        }

        // Create a new job.


        // Specify various job-specific parameters.
        Job job = this.getJob(args[0], args[1]);
        job.waitForCompletion(true);
        return 0;
    }
}
