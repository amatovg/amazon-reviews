package amazon.graphbuilder;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import amazon.AmazonReview;

public class FirstBuildMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();
    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {

        String record = inputValue.toString();
        AmazonReview review = AmazonReview.fromJson(record);
        if(!review.userId.equals("unknown")){
            String title = review.title.replace("\t", " ");
            outputKey.set(review.productId+"|"+title);
            outputValue.set((int)review.score+"|"+review.userId);
            context.write(outputKey, outputValue);
        }
    }

}
