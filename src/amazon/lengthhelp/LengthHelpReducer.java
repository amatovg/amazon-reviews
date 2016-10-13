package amazon.lengthhelp;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class LengthHelpReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

    DoubleWritable outputValue = new DoubleWritable();

    public void reduce(IntWritable inputKey, Iterable<DoubleWritable> inputValues, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        int size=0;
        for (DoubleWritable input: inputValues) {
            double val = input.get();
            sum += val;
            size++;
        }
        if(size>=3){
            double avg = sum / size;
            outputValue.set(avg);
            context.write(inputKey, outputValue);
        }
    }

}
