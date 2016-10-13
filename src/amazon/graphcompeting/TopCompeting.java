package amazon.graphcompeting;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopCompeting extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new TopCompeting(), args);
        System.exit(res);
    }

    public Job getJob(String input, String titles, String output) throws IOException {
        // Create a new job.
        Job job = Job.getInstance(this.getConf());
        job.setJarByClass(TopCompeting.class);
        job.setJobName("Top Competitors");

        // set mapper / reducer classes.
        job.setMapperClass(TopCompetingMapper.class);
        job.setReducerClass(TopCompetingReducer.class);

        // Define output types.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Define input and output folders.
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(new Path(output), true);
        FileInputFormat.addInputPath(job, new Path(input));
        FileInputFormat.addInputPath(job, new Path(titles));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job;
    }

    @Override
    public int run(String[] args) throws Exception{

        if (args.length != 3 && args.length !=4) {
            System.err.println("Usage: CMD <input path> <titles> <output path>");
            System.exit(-1);
        }
        Job job = this.getJob(args[0], args[1], args[2]);
        job.waitForCompletion(true);
        return 0;


    }
}
