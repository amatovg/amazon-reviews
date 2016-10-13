package amazon.lengthhelp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LengthHelp extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new LengthHelp(), args);
        System.exit(res);
    }
    public Job getJob(String input, String output, String type) throws IOException {
        // Create a new job.
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(LengthHelp.class);
        job.setJobName("Length Help");

        // set mapper / reducer classes.
        job.setMapperClass(LengthHelpMapper.class);
        job.setReducerClass(LengthHelpReducer.class);

        // Define output types.
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);

        // Specify various job-specific parameters.
        job.getConfiguration().set("type", type);

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
            System.err.println("Usage: CMD <input path> <output path> <unit>");
            return 0;
        }
        Job job = this.getJob(args[0], args[1],args[2]);
        job.waitForCompletion(true);
        return 0;


    }
}
