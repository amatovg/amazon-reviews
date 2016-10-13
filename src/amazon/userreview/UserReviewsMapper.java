package amazon.userreview;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import amazon.AmazonReview;

public class UserReviewsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outputKey = new Text();
    private IntWritable outputValue = new IntWritable(1);
    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {

        String record = inputValue.toString();
        AmazonReview review = AmazonReview.fromJson(record);
        outputKey.set(review.userId);
        context.write(outputKey, outputValue);
    }

}
