package amazon.userreview;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UserReviews extends Configured implements Tool {

    public Job getJob(String inputPath, String outputPath)
            throws IOException {
        // Create a new job.
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(UserReviews.class);
        job.setJobName("User Reviews");

        // set mapper / reducer classes.
        job.setMapperClass(UserReviewsMapper.class);
        job.setReducerClass(UserReviewsReducer.class);

        // Define output types.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);

        // Define input and output folders.
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(outputPath), true);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new UserReviews(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception{

        if (args.length != 2) {
            System.err.println("Usage: CMD <input path> <output path>");
            return 0;
        }
        Job job = this.getJob(args[0], args[1]);
        job.waitForCompletion(true);
        return 0;


    }
}
