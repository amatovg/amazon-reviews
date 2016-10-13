package amazon.reviewtime;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import amazon.AmazonReview;

public class ReviewsTimeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text outputKey = new Text();
    private final IntWritable outputValue = new IntWritable(1);

    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {

        String record = inputValue.toString();
        AmazonReview review = AmazonReview.fromJson(record);
        Calendar date = review.getCalendar();

        String unit = context.getConfiguration().get("unit");

        if(unit.equals("year")){
            int year = date.get(Calendar.YEAR);
            outputKey.set(""+year);
        } else if(unit.equals("month")){
            outputKey.set(new SimpleDateFormat("MM").format(date.getTime()));
        } else if(unit.equals("year-month")){
            outputKey.set(new SimpleDateFormat("yyyy-MM").format(date.getTime()));
        } else{
            System.err.println("Only allowed values for unit are \"year\", \"month\", \"year-month\"");
            System.exit(-1);
        }

        context.write(outputKey, outputValue);
    }

}
