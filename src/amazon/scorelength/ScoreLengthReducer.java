package amazon.scorelength;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class ScoreLengthReducer extends Reducer<IntWritable, IntWritable, IntWritable, DoubleWritable> {

    DoubleWritable outputValue = new DoubleWritable();

    public void reduce(IntWritable inputKey, Iterable<IntWritable> inputValues, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        int size=0;
        for (IntWritable input: inputValues) {
            int val = input.get();
            sum += val;
            size++;
        }
        if(size>=5){
            double avg = (double) sum / size;
            outputValue.set(avg);
            context.write(inputKey, outputValue);
        }
    }

}
