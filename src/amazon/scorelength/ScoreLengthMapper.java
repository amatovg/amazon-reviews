package amazon.scorelength;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

import amazon.AmazonReview;

public class ScoreLengthMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable outputKey = new IntWritable();
    private IntWritable outputValue = new IntWritable();
    private static final Splitter SPLITTER = Splitter.on(
            CharMatcher.anyOf(" ,;:\".\t\n\r\f'")).omitEmptyStrings();

    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {

        String json = inputValue.toString();
        AmazonReview review = AmazonReview.fromJson(json);

        String type = context.getConfiguration().get("type");
        int count = 0;

        if (type.equals("word")) {
            for (@SuppressWarnings("unused")
            String word : SPLITTER.split(review.text)) {
                count++;
            }
        } else if (type.equals("char")) {
            count = review.text.length();

        } else {
            System.err
                    .println("Only allowed values for type are \"word\" and \"char\"");
            System.exit(-1);
        }

        int precision = (int) Math.pow(10, (int) Math.log10(count) - 1);
        if(precision <= 0)
            precision = 1;

        count = Math.round(count / precision) * precision;

        outputKey.set(count);
        outputValue.set((int) review.score);
        context.write(outputKey, outputValue);
    }

}
