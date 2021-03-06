package amazon.scorecount;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private IntWritable outputValue = new IntWritable();

    public void reduce(IntWritable inputKey, Iterable<IntWritable> inputValues, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value: inputValues) {
            sum += value.get();
        }

        this.outputValue.set(sum);
        context.write(inputKey, outputValue);
    }

}
