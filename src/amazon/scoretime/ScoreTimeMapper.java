package amazon.scoretime;


import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import amazon.AmazonReview;

public class ScoreTimeMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text outputKey = new Text();
    private DoubleWritable outputValue = new DoubleWritable();

    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {

        String json = inputValue.toString();
        AmazonReview review = AmazonReview.fromJson(json);
        outputKey.set(new SimpleDateFormat("yyyy-MM").format(review.getCalendar().getTime()));
        outputValue.set(review.score);
        context.write(outputKey, outputValue);
    }

}
