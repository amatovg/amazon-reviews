package amazon.scoretime;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ScoreTime extends Configured implements Tool {

    public Job getJob(String input, String output) throws IOException {
        // Create a new job.
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(ScoreTime.class);
        job.setJobName("Score Time");

        // set mapper / reducer classes.
        job.setMapperClass(ScoreTimeMapper.class);
        job.setReducerClass(ScoreTimeReducer.class);

        // Define output types.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);

        // Define input and output folders.
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(output), true);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ScoreTime(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CMD <input path> <output path>");
            return 0;
        }

        Job job = this.getJob(args[0], args[1]);
        job.waitForCompletion(true);
        return 0;
    }
}
