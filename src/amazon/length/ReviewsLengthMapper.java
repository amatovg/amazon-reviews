package amazon.length;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import amazon.AmazonReview;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

public class ReviewsLengthMapper extends
        Mapper<LongWritable, Text, Text, IntWritable> {

    private static final Splitter SPLITTER = Splitter.on(
            CharMatcher.anyOf(" ,;:\".\t\n\r\f'")).omitEmptyStrings();

    private Text outputKey = new Text();
    private final IntWritable outputValue = new IntWritable(1);

    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {
        String record = inputValue.toString();
        AmazonReview review = AmazonReview.fromJson(record);

        int precision = 0;
        try {
            precision = Math.abs(Integer.parseInt(context.getConfiguration()
                    .get("precision")));
        } catch (NumberFormatException e) {
            System.err
                    .println("Only integer values are allowed for precision.");
            System.exit(-1);
        }
        String type = context.getConfiguration().get("type");

        if (type.equals("word")) {
            int count = 0;
            for (@SuppressWarnings("unused")
            String word : SPLITTER.split(review.text)) {
                count++;
            }
            outputKey.set(Math.round(count / precision) * precision + "");

        } else if (type.equals("char")) {
            outputKey.set(Math.round(review.text.length() / precision) * precision
                    + "");

        } else {
            System.err
                    .println("Only allowed values for type are \"word\" and \"char\"");
            System.exit(-1);
        }

        context.write(outputKey, outputValue);

    }

}
