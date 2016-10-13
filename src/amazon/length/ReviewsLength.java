package amazon.length;

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

public class ReviewsLength extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new ReviewsLength(), args);
        System.exit(res);
    }

    public Job getJob(String input, String output, String type, String precision) throws IOException {
        // Create a new job.
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(ReviewsLength.class);
        job.setJobName("Review Length");

        // set mapper / reducer classes.
        job.setMapperClass(ReviewsLengthMapper.class);
        job.setReducerClass(ReviewsLengthReducer.class);
        job.setCombinerClass(ReviewsLengthReducer.class);

        // Define output types.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Define input and output folders.
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(output), true);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // Specify various job-specific parameters.
        job.getConfiguration().set("type", type);
        job.getConfiguration().set("precision", precision);
        return job;
    }

    @Override
    public int run(String[] args) throws Exception{

        if (args.length != 4) {
            System.err.println("Usage: CMD <input path> <output path> <type> <precision>");
            System.exit(-1);
        }
        Job job = this.getJob(args[0], args[1], args[2], args[3]);
        job.waitForCompletion(true);
        return 0;


    }
}
