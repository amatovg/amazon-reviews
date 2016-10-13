package amazon.scorecount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import amazon.AmazonReview;

public class ScoreCountMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable outputKey = new IntWritable();
    private final IntWritable outputValue = new IntWritable(1);

    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {

        String record = inputValue.toString();
        AmazonReview review = AmazonReview.fromJson(record);
        int score = (int) review.score;


        outputKey.set(score);
        context.write(outputKey, outputValue);
    }

}
