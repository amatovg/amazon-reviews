package amazon.userreview;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CumulateMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private IntWritable outputKey = new IntWritable();
    private IntWritable outputValue = new IntWritable(1);
    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context)
            throws IOException, InterruptedException {

        String line = inputValue.toString();
        int count = Integer.parseInt(line.split("\t")[1]);
        outputKey.set(count);
        context.write(outputKey, outputValue);
    }

}
