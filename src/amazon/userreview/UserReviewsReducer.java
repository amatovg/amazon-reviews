package amazon.userreview;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserReviewsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable outputValue = new IntWritable();

    public void reduce(Text inputKey, Iterable<IntWritable> inputValues, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value: inputValues)
            sum += value.get();
        this.outputValue.set(sum);
        context.write(inputKey, outputValue);
    }

}
